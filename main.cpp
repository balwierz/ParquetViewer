#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/type.h>
#include <arrow/scalar.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

// ── CLI args ─────────────────────────────────────────────────────────────────

struct Config {
    std::string path;
    int         head_rows   = 10;
    int         max_col_w   = 32;   // max chars per cell (truncated with …)
    int         max_cols    = 0;    // 0 = all
    bool        no_index    = false;
};

static void print_usage(const char* prog) {
    std::fprintf(stderr,
        "Usage: %s [options] <file.parquet>\n"
        "  -n <rows>   number of rows to display (default 10, 0 = all)\n"
        "  -w <width>  max column cell width     (default 32)\n"
        "  -c <cols>   max columns to show       (default all)\n"
        "  --no-index  suppress row-index column\n"
        "  -h          show this help\n",
        prog);
}

static Config parse_args(int argc, char** argv) {
    Config cfg;
    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "-h") || !std::strcmp(argv[i], "--help")) {
            print_usage(argv[0]); std::exit(0);
        } else if (!std::strcmp(argv[i], "--no-index")) {
            cfg.no_index = true;
        } else if (!std::strcmp(argv[i], "-n") && i + 1 < argc) {
            cfg.head_rows = std::atoi(argv[++i]);
        } else if (!std::strcmp(argv[i], "-w") && i + 1 < argc) {
            cfg.max_col_w = std::max(3, std::atoi(argv[++i]));
        } else if (!std::strcmp(argv[i], "-c") && i + 1 < argc) {
            cfg.max_cols = std::atoi(argv[++i]);
        } else if (argv[i][0] != '-') {
            if (positional++ == 0) cfg.path = argv[i];
        } else {
            std::fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]); std::exit(1);
        }
    }
    if (cfg.path.empty()) { print_usage(argv[0]); std::exit(1); }
    return cfg;
}

// ── Value formatting ──────────────────────────────────────────────────────────

static bool is_numeric_type(arrow::Type::type t) {
    switch (t) {
        case arrow::Type::INT8:    case arrow::Type::INT16:
        case arrow::Type::INT32:   case arrow::Type::INT64:
        case arrow::Type::UINT8:   case arrow::Type::UINT16:
        case arrow::Type::UINT32:  case arrow::Type::UINT64:
        case arrow::Type::FLOAT:   case arrow::Type::DOUBLE:
        case arrow::Type::DECIMAL128: case arrow::Type::DECIMAL256:
        case arrow::Type::DATE32:  case arrow::Type::DATE64:
        case arrow::Type::DURATION:
        case arrow::Type::TIME32:  case arrow::Type::TIME64:
        case arrow::Type::TIMESTAMP:
            return true;
        default: return false;
    }
}

// Truncate to max_w *display* characters.
// We assume content is ASCII or that bytes ≈ display chars (good enough for a preview).
// Using ASCII "…" (three dots) keeps byte count == display width.
static std::string truncate(const std::string& s, int max_w) {
    if (max_w < 4) max_w = 4;
    if ((int)s.size() <= max_w) return s;
    return s.substr(0, max_w - 3) + "...";
}

// Return the display width of a string (bytes for ASCII/Latin; approximation for UTF-8).
// Good enough for column alignment in a terminal preview.
static int display_width(const std::string& s) {
    return (int)s.size();  // fast path: treat as ASCII
}

static std::string cell_to_string(const arrow::Array& arr, int64_t row) {
    if (arr.IsNull(row)) return "null";

    switch (arr.type_id()) {
        case arrow::Type::BOOL:
            return static_cast<const arrow::BooleanArray&>(arr).Value(row) ? "true" : "false";
        case arrow::Type::INT8:
            return std::to_string(static_cast<const arrow::Int8Array&>(arr).Value(row));
        case arrow::Type::INT16:
            return std::to_string(static_cast<const arrow::Int16Array&>(arr).Value(row));
        case arrow::Type::INT32:
            return std::to_string(static_cast<const arrow::Int32Array&>(arr).Value(row));
        case arrow::Type::INT64:
            return std::to_string(static_cast<const arrow::Int64Array&>(arr).Value(row));
        case arrow::Type::UINT8:
            return std::to_string(static_cast<const arrow::UInt8Array&>(arr).Value(row));
        case arrow::Type::UINT16:
            return std::to_string(static_cast<const arrow::UInt16Array&>(arr).Value(row));
        case arrow::Type::UINT32:
            return std::to_string(static_cast<const arrow::UInt32Array&>(arr).Value(row));
        case arrow::Type::UINT64:
            return std::to_string(static_cast<const arrow::UInt64Array&>(arr).Value(row));
        case arrow::Type::FLOAT: {
            std::ostringstream ss;
            ss << std::setprecision(6) << static_cast<const arrow::FloatArray&>(arr).Value(row);
            return ss.str();
        }
        case arrow::Type::DOUBLE: {
            std::ostringstream ss;
            ss << std::setprecision(8) << static_cast<const arrow::DoubleArray&>(arr).Value(row);
            return ss.str();
        }
        case arrow::Type::STRING: {
            auto& a = static_cast<const arrow::StringArray&>(arr);
            return a.GetString(row);
        }
        case arrow::Type::LARGE_STRING: {
            auto& a = static_cast<const arrow::LargeStringArray&>(arr);
            return a.GetString(row);
        }
        case arrow::Type::BINARY: {
            auto& a = static_cast<const arrow::BinaryArray&>(arr);
            return "<binary " + std::to_string(a.value_length(row)) + "B>";
        }
        case arrow::Type::LARGE_BINARY: {
            auto& a = static_cast<const arrow::LargeBinaryArray&>(arr);
            return "<binary " + std::to_string(a.value_length(row)) + "B>";
        }
        case arrow::Type::DICTIONARY: {
            // Decode index → look up actual value in the dictionary array.
            // DictionaryScalar::ToString() dumps the entire dictionary, so we
            // do the lookup manually.
            auto& dict_arr = static_cast<const arrow::DictionaryArray&>(arr);
            auto  dict     = dict_arr.dictionary();   // values array
            auto  indices  = dict_arr.indices();      // integer index array

            int64_t idx = -1;
            switch (indices->type_id()) {
                case arrow::Type::INT8:   idx = static_cast<const arrow::Int8Array&>(*indices).Value(row);   break;
                case arrow::Type::INT16:  idx = static_cast<const arrow::Int16Array&>(*indices).Value(row);  break;
                case arrow::Type::INT32:  idx = static_cast<const arrow::Int32Array&>(*indices).Value(row);  break;
                case arrow::Type::INT64:  idx = static_cast<const arrow::Int64Array&>(*indices).Value(row);  break;
                case arrow::Type::UINT8:  idx = static_cast<const arrow::UInt8Array&>(*indices).Value(row);  break;
                case arrow::Type::UINT16: idx = static_cast<const arrow::UInt16Array&>(*indices).Value(row); break;
                case arrow::Type::UINT32: idx = static_cast<const arrow::UInt32Array&>(*indices).Value(row); break;
                case arrow::Type::UINT64: idx = static_cast<int64_t>(static_cast<const arrow::UInt64Array&>(*indices).Value(row)); break;
                default: break;
            }
            if (idx >= 0 && idx < dict->length())
                return cell_to_string(*dict, idx);
            return "null";
        }
        default: {
            auto scalar_res = arr.GetScalar(row);
            if (scalar_res.ok()) return scalar_res.ValueOrDie()->ToString();
            return "?";
        }
    }
}

// ── ASCII table drawing ───────────────────────────────────────────────────────

struct Column {
    std::string              header;
    bool                     right_align;
    std::vector<std::string> cells;
    int                      width;
};

static void draw_separator(const std::vector<Column>& cols) {
    std::putchar('+');
    for (auto& c : cols) {
        for (int i = 0; i < c.width + 2; ++i) std::putchar('-');
        std::putchar('+');
    }
    std::putchar('\n');
}

static void draw_row(const std::vector<Column>& cols,
                     const std::vector<std::string>& vals,
                     const std::vector<bool>& right_align) {
    std::putchar('|');
    for (std::size_t i = 0; i < cols.size(); ++i) {
        int pad = cols[i].width - display_width(vals[i]);
        if (right_align[i]) {
            std::printf(" %*s%s |", pad, "", vals[i].c_str());
        } else {
            std::printf(" %s%*s |", vals[i].c_str(), pad, "");
        }
    }
    std::putchar('\n');
}

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    Config cfg = parse_args(argc, argv);

    // --- Open parquet file ---
    auto maybe_file = arrow::io::ReadableFile::Open(cfg.path);
    if (!maybe_file.ok()) {
        std::fprintf(stderr, "Error opening '%s': %s\n",
                     cfg.path.c_str(), maybe_file.status().ToString().c_str());
        return 1;
    }
    auto infile = maybe_file.ValueOrDie();

    // Build reader with properties
    parquet::ArrowReaderProperties arrow_props = parquet::default_arrow_reader_properties();
    arrow_props.set_pre_buffer(true);
    if (cfg.head_rows > 0) arrow_props.set_batch_size(cfg.head_rows);

    parquet::arrow::FileReaderBuilder builder;
    auto open_st = builder.Open(infile);
    if (!open_st.ok()) {
        std::fprintf(stderr, "Error opening parquet: %s\n", open_st.ToString().c_str());
        return 1;
    }
    builder.memory_pool(arrow::default_memory_pool());
    builder.properties(arrow_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    auto build_st = builder.Build(&arrow_reader);
    if (!build_st.ok()) {
        std::fprintf(stderr, "Error building reader: %s\n", build_st.ToString().c_str());
        return 1;
    }

    auto* pq_reader  = arrow_reader->parquet_reader();
    auto  file_meta  = pq_reader->metadata();
    int64_t total_rows = file_meta->num_rows();
    int     num_cols   = file_meta->num_columns();
    int     num_rg     = file_meta->num_row_groups();

    std::shared_ptr<arrow::Schema> schema;
    auto schema_st = arrow_reader->GetSchema(&schema);
    if (!schema_st.ok()) {
        std::fprintf(stderr, "Error reading schema: %s\n", schema_st.ToString().c_str());
        return 1;
    }

    // --- Determine which columns to show ---
    int show_cols = (cfg.max_cols > 0)
                    ? std::min(cfg.max_cols, (int)schema->num_fields())
                    : (int)schema->num_fields();

    // --- Read only enough row groups for head_rows ---
    int64_t rows_wanted = (cfg.head_rows <= 0) ? total_rows : (int64_t)cfg.head_rows;

    std::vector<int> col_indices;
    col_indices.reserve(show_cols);
    for (int i = 0; i < show_cols; ++i) col_indices.push_back(i);

    std::vector<int> rg_ids;
    {
        int64_t acc = 0;
        for (int rg = 0; rg < num_rg && (cfg.head_rows <= 0 || acc < rows_wanted); ++rg) {
            rg_ids.push_back(rg);
            acc += file_meta->RowGroup(rg)->num_rows();
        }
    }

    std::shared_ptr<arrow::Table> table;
    auto read_st = arrow_reader->ReadRowGroups(rg_ids, col_indices, &table);
    if (!read_st.ok()) {
        std::fprintf(stderr, "Error reading data: %s\n", read_st.ToString().c_str());
        return 1;
    }

    if (cfg.head_rows > 0 && table->num_rows() > rows_wanted)
        table = table->Slice(0, rows_wanted);
    int64_t n_display = table->num_rows();

    // --- Build column display structs ---
    std::vector<Column> columns;
    columns.reserve(show_cols + 1);

    if (!cfg.no_index) {
        Column idx;
        idx.header      = "";
        idx.right_align = true;
        int digits = 1;
        for (int64_t v = std::max<int64_t>(n_display - 1, 0); v >= 10; v /= 10) ++digits;
        idx.width = digits;
        idx.cells.reserve(n_display);
        for (int64_t r = 0; r < n_display; ++r)
            idx.cells.push_back(std::to_string(r));
        columns.push_back(std::move(idx));
    }

    for (int ci = 0; ci < show_cols; ++ci) {
        auto field   = schema->field(ci);
        auto arr_col = table->column(ci);

        Column col;
        col.header      = field->name();
        col.right_align = is_numeric_type(field->type()->id());
        col.width       = std::max(display_width(col.header), 4);
        col.cells.reserve(n_display);

        for (auto& chunk : arr_col->chunks()) {
            for (int64_t r = 0; r < chunk->length(); ++r) {
                std::string val = truncate(cell_to_string(*chunk, r), cfg.max_col_w);
                if (display_width(val) > col.width) col.width = display_width(val);
                col.cells.push_back(std::move(val));
            }
        }
        col.width  = std::min(col.width, cfg.max_col_w);
        col.header = truncate(col.header, cfg.max_col_w);
        col.width  = std::max(col.width, display_width(col.header));
        columns.push_back(std::move(col));
    }

    bool cols_truncated = (show_cols < num_cols);

    // --- Draw table ---
    draw_separator(columns);

    {
        std::vector<std::string> hdr;
        std::vector<bool> ra;
        for (auto& c : columns) { hdr.push_back(c.header); ra.push_back(false); }
        draw_row(columns, hdr, ra);
    }
    draw_separator(columns);

    for (int64_t r = 0; r < n_display; ++r) {
        std::vector<std::string> row;
        std::vector<bool> ra;
        for (auto& c : columns) {
            row.push_back(c.cells[r]);
            ra.push_back(c.right_align);
        }
        draw_row(columns, row, ra);
    }
    draw_separator(columns);

    if (cols_truncated)
        std::printf("  ... %d more column(s) not shown (-c 0 to see all)\n",
                    num_cols - show_cols);

    // --- Summary ---
    std::printf("\n[%lld rows x %d columns]\n", (long long)total_rows, num_cols);

    // Schema table
    int name_w = 6;  // "Column"
    int type_w = 4;  // "Type"
    for (int ci = 0; ci < (int)schema->num_fields(); ++ci) {
        auto f = schema->field(ci);
        name_w = std::max(name_w, (int)f->name().size());
        type_w = std::max(type_w, (int)f->type()->ToString().size());
    }
    name_w = std::min(name_w, 40);
    type_w = std::min(type_w, 40);

    std::printf("\n%-*s  %-*s  Nullable\n", name_w, "Column", type_w, "Type");
    std::printf("%s  %s  --------\n",
                std::string(name_w, '-').c_str(),
                std::string(type_w, '-').c_str());
    for (int ci = 0; ci < (int)schema->num_fields(); ++ci) {
        auto f = schema->field(ci);
        std::string fname = truncate(f->name(), name_w);
        std::string ftype = truncate(f->type()->ToString(), type_w);
        std::printf("%-*s  %-*s  %s\n",
                    name_w, fname.c_str(),
                    type_w, ftype.c_str(),
                    f->nullable() ? "yes" : "no");
    }

    // File metadata
    int64_t total_size = 0;
    for (int rg = 0; rg < num_rg; ++rg)
        total_size += file_meta->RowGroup(rg)->total_compressed_size();

    char size_buf[32];
    if (total_size < 1024)
        std::snprintf(size_buf, sizeof(size_buf), "%lld B", (long long)total_size);
    else if (total_size < 1024 * 1024)
        std::snprintf(size_buf, sizeof(size_buf), "%.1f KiB", total_size / 1024.0);
    else if (total_size < 1024LL * 1024 * 1024)
        std::snprintf(size_buf, sizeof(size_buf), "%.2f MiB", total_size / (1024.0 * 1024));
    else
        std::snprintf(size_buf, sizeof(size_buf), "%.2f GiB", total_size / (1024.0 * 1024 * 1024));

    std::printf("\nFile: %s\n", cfg.path.c_str());
    std::printf("Row groups: %d  |  Compressed size: %s\n", num_rg, size_buf);
    if (!file_meta->created_by().empty())
        std::printf("Created by: %s\n", file_meta->created_by().c_str());

    return 0;
}
