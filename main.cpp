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
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

// ── Colors ────────────────────────────────────────────────────────────────────

struct Colors {
    const char* reset      = "";
    const char* border     = "";   // table lines
    const char* header     = "";   // column name row
    const char* row_idx    = "";   // row index column
    const char* null_val   = "";   // null cells
    const char* number     = "";   // numeric / temporal values
    const char* bool_true  = "";   // true
    const char* bool_false = "";   // false
    const char* trunc      = "";   // the "..." suffix
    const char* type_int   = "";   // schema: integer type names
    const char* type_float = "";   // schema: float type names
    const char* type_str   = "";   // schema: string/binary type names
    const char* type_time  = "";   // schema: temporal type names
    const char* type_bool  = "";   // schema: bool type names
    const char* type_other = "";   // schema: everything else
    const char* meta_key   = "";   // summary labels ("File:", "Row groups:", …)
};

static Colors g_color;  // populated by init_colors()

static void init_colors() {
    g_color.reset      = "\033[0m";
    g_color.border     = "\033[90m";       // dark gray
    g_color.header     = "\033[1;97m";     // bold bright-white
    g_color.row_idx    = "\033[90m";       // dark gray
    g_color.null_val   = "\033[2;3m";      // dim + italic
    g_color.number     = "\033[96m";       // bright cyan
    g_color.bool_true  = "\033[92m";       // bright green
    g_color.bool_false = "\033[33m";       // yellow
    g_color.trunc      = "\033[90m";       // dark gray for "..."
    g_color.type_int   = "\033[96m";       // bright cyan
    g_color.type_float = "\033[93m";       // bright yellow
    g_color.type_str   = "\033[92m";       // bright green
    g_color.type_time  = "\033[95m";       // bright magenta
    g_color.type_bool  = "\033[94m";       // bright blue
    g_color.type_other = "\033[37m";       // white
    g_color.meta_key   = "\033[1m";        // bold
}

// Pick the right color for an Arrow type in the schema summary
static const char* type_color(arrow::Type::type t) {
    switch (t) {
        case arrow::Type::INT8:  case arrow::Type::INT16:
        case arrow::Type::INT32: case arrow::Type::INT64:
        case arrow::Type::UINT8: case arrow::Type::UINT16:
        case arrow::Type::UINT32: case arrow::Type::UINT64:
        case arrow::Type::DECIMAL128: case arrow::Type::DECIMAL256:
            return g_color.type_int;
        case arrow::Type::FLOAT: case arrow::Type::DOUBLE:
        case arrow::Type::HALF_FLOAT:
            return g_color.type_float;
        case arrow::Type::STRING: case arrow::Type::LARGE_STRING:
        case arrow::Type::BINARY: case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
            return g_color.type_str;
        case arrow::Type::DATE32: case arrow::Type::DATE64:
        case arrow::Type::TIME32: case arrow::Type::TIME64:
        case arrow::Type::TIMESTAMP: case arrow::Type::DURATION:
            return g_color.type_time;
        case arrow::Type::BOOL:
            return g_color.type_bool;
        default:
            return g_color.type_other;
    }
}

// Unwrap dictionary to its value type for coloring purposes
static arrow::Type::type display_type(const arrow::Field& f) {
    auto t = f.type();
    if (t->id() == arrow::Type::DICTIONARY)
        return std::static_pointer_cast<arrow::DictionaryType>(t)->value_type()->id();
    return t->id();
}

// ── CLI args ─────────────────────────────────────────────────────────────────

enum class ColorMode { Auto, Always, Never };

struct Config {
    std::string path;
    int         head_rows  = 10;
    int         max_col_w  = 32;
    int         max_cols   = 0;
    bool        no_index   = false;
    ColorMode   color      = ColorMode::Auto;
    char        delimiter  = 0;    // 0 = table mode; '\t' or ',' = delimited output
    bool        no_header  = false;
};

static void print_usage(const char* prog) {
    std::fprintf(stderr,
        "Usage: %s [options] <file.parquet>\n"
        "\nTable options:\n"
        "  -n <rows>          number of rows to display (default 10, 0 = all)\n"
        "  -w <width>         max column cell width     (default 32)\n"
        "  -c <cols>          max columns to show       (default all)\n"
        "  --no-index         suppress row-index column\n"
        "  --color[=WHEN]     colorize output: auto (default), always, never\n"
        "\nDelimited output (replaces table view):\n"
        "  --tsv              write tab-separated values to stdout\n"
        "  --csv              write comma-separated values to stdout\n"
        "  --delimiter <sep>  write with a custom single-character delimiter\n"
        "  --no-header        omit the header row from delimited output\n"
        "  (with delimited output -n defaults to all rows; -c still applies)\n"
        "\n  -h                 show this help\n",
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
        } else if (!std::strcmp(argv[i], "--no-header")) {
            cfg.no_header = true;
        } else if (!std::strcmp(argv[i], "-n") && i + 1 < argc) {
            cfg.head_rows = std::atoi(argv[++i]);
        } else if (!std::strcmp(argv[i], "-w") && i + 1 < argc) {
            cfg.max_col_w = std::max(4, std::atoi(argv[++i]));
        } else if (!std::strcmp(argv[i], "-c") && i + 1 < argc) {
            cfg.max_cols = std::atoi(argv[++i]);
        } else if (!std::strcmp(argv[i], "--color") ||
                   !std::strcmp(argv[i], "--color=auto")) {
            cfg.color = ColorMode::Auto;
        } else if (!std::strcmp(argv[i], "--color=always")) {
            cfg.color = ColorMode::Always;
        } else if (!std::strcmp(argv[i], "--color=never")) {
            cfg.color = ColorMode::Never;
        } else if (!std::strcmp(argv[i], "--tsv")) {
            cfg.delimiter = '\t';
        } else if (!std::strcmp(argv[i], "--csv")) {
            cfg.delimiter = ',';
        } else if (!std::strcmp(argv[i], "--delimiter") && i + 1 < argc) {
            const char* sep = argv[++i];
            if (!std::strcmp(sep, "tab"))   cfg.delimiter = '\t';
            else if (!std::strcmp(sep, "comma")) cfg.delimiter = ',';
            else if (sep[0] && !sep[1])     cfg.delimiter = sep[0];
            else { std::fprintf(stderr, "delimiter must be a single character\n"); std::exit(1); }
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

static std::string truncate(const std::string& s, int max_w) {
    if (max_w < 4) max_w = 4;
    if ((int)s.size() <= max_w) return s;
    return s.substr(0, max_w - 3) + "...";
}

static int display_width(const std::string& s) {
    return (int)s.size();
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
        case arrow::Type::STRING:
            return static_cast<const arrow::StringArray&>(arr).GetString(row);
        case arrow::Type::LARGE_STRING:
            return static_cast<const arrow::LargeStringArray&>(arr).GetString(row);
        case arrow::Type::BINARY: {
            auto& a = static_cast<const arrow::BinaryArray&>(arr);
            return "<binary " + std::to_string(a.value_length(row)) + "B>";
        }
        case arrow::Type::LARGE_BINARY: {
            auto& a = static_cast<const arrow::LargeBinaryArray&>(arr);
            return "<binary " + std::to_string(a.value_length(row)) + "B>";
        }
        case arrow::Type::DICTIONARY: {
            auto& dict_arr = static_cast<const arrow::DictionaryArray&>(arr);
            auto  dict     = dict_arr.dictionary();
            auto  indices  = dict_arr.indices();
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
            auto res = arr.GetScalar(row);
            return res.ok() ? res.ValueOrDie()->ToString() : "?";
        }
    }
}

// ── ASCII table drawing ───────────────────────────────────────────────────────

struct Column {
    std::string              header;
    bool                     right_align;
    bool                     is_index = false;
    bool                     is_bool  = false;
    std::vector<std::string> cells;
    int                      width;
};

static void draw_separator(const std::vector<Column>& cols) {
    std::printf("%s+", g_color.border);
    for (auto& c : cols) {
        for (int i = 0; i < c.width + 2; ++i) std::putchar('-');
        std::putchar('+');
    }
    std::printf("%s\n", g_color.reset);
}

// Emit one cell with color, proper padding, but no border characters.
// Returns nothing; writes directly to stdout.
static void emit_cell(const Column& col, const std::string& val,
                      bool right_align, bool is_header) {
    int pad = col.width - display_width(val);

    // Choose foreground color for the content
    const char* fg = "";
    if (*g_color.reset) {
        if (is_header) {
            fg = g_color.header;
        } else if (col.is_index) {
            fg = g_color.row_idx;
        } else if (val == "null") {
            fg = g_color.null_val;
        } else if (col.is_bool) {
            fg = (val == "true") ? g_color.bool_true : g_color.bool_false;
        } else if (right_align) {
            fg = g_color.number;
        }
    }

    // For truncated values, split the "..." suffix and dim it separately
    bool truncated = !is_header && val.size() >= 3 &&
                     val.compare(val.size() - 3, 3, "...") == 0;

    if (right_align) {
        std::printf(" %*s", pad, "");   // leading spaces (no color)
        if (truncated) {
            std::printf("%s%.*s%s%s%s%s",
                fg, (int)val.size() - 3, val.c_str(),   // body
                g_color.reset, g_color.trunc, "...", g_color.reset);
        } else {
            std::printf("%s%s%s", fg, val.c_str(), *fg ? g_color.reset : "");
        }
    } else {
        if (truncated) {
            std::printf(" %s%.*s%s%s%s%s%*s",
                fg, (int)val.size() - 3, val.c_str(),   // body
                g_color.reset, g_color.trunc, "...", g_color.reset,
                pad, "");
        } else {
            std::printf(" %s%s%s%*s",
                fg, val.c_str(), *fg ? g_color.reset : "", pad, "");
        }
    }
}

static void draw_row(const std::vector<Column>& cols,
                     const std::vector<std::string>& vals,
                     const std::vector<bool>& right_align,
                     bool is_header = false) {
    for (std::size_t i = 0; i < cols.size(); ++i) {
        std::printf("%s|%s", g_color.border, g_color.reset);
        emit_cell(cols[i], vals[i], right_align[i], is_header);
        std::printf(" ");
    }
    std::printf("%s|%s\n", g_color.border, g_color.reset);
}

// ── Delimited output ─────────────────────────────────────────────────────────

// RFC 4180 quoting: wrap in double-quotes if the value contains the delimiter,
// a double-quote, or a newline; escape embedded quotes by doubling them.
static void write_csv_field(const std::string& val, char sep) {
    bool needs_quote = val.find(sep)  != std::string::npos ||
                       val.find('"')  != std::string::npos ||
                       val.find('\n') != std::string::npos ||
                       val.find('\r') != std::string::npos;
    if (!needs_quote) {
        std::fputs(val.c_str(), stdout);
        return;
    }
    std::putchar('"');
    for (char c : val) {
        if (c == '"') std::putchar('"');   // double the quote
        std::putchar(c);
    }
    std::putchar('"');
}

// Stream the full file (or up to head_rows) as delimited text, one row group
// at a time so memory use stays bounded regardless of file size.
static void write_delimited(parquet::arrow::FileReader* reader,
                             const std::shared_ptr<arrow::Schema>& schema,
                             const parquet::FileMetaData* meta,
                             const Config& cfg) {
    char sep = cfg.delimiter;
    int  show_cols  = (cfg.max_cols > 0)
                      ? std::min(cfg.max_cols, (int)schema->num_fields())
                      : (int)schema->num_fields();
    // In delimiter mode default to all rows unless -n was given explicitly.
    // We detect "explicitly given" by checking head_rows != the sentinel 10…
    // Simpler: just honour whatever head_rows holds (caller sets 0 = all).
    int64_t rows_left = (cfg.head_rows <= 0) ? INT64_MAX : (int64_t)cfg.head_rows;

    std::vector<int> col_indices;
    for (int i = 0; i < show_cols; ++i) col_indices.push_back(i);

    // Header row
    if (!cfg.no_header) {
        for (int ci = 0; ci < show_cols; ++ci) {
            if (ci) std::putchar(sep);
            write_csv_field(schema->field(ci)->name(), sep);
        }
        std::putchar('\n');
    }

    // Data rows — one row group at a time
    for (int rg = 0; rg < meta->num_row_groups() && rows_left > 0; ++rg) {
        std::shared_ptr<arrow::Table> table;
        auto st = reader->ReadRowGroups({rg}, col_indices, &table);
        if (!st.ok()) {
            std::fprintf(stderr, "Warning: error reading row group %d: %s\n",
                         rg, st.ToString().c_str());
            continue;
        }

        int64_t rg_rows = std::min(table->num_rows(), rows_left);
        rows_left -= rg_rows;

        // Flatten each column's chunks into a single array list we can index
        // cheaply without copying data.
        struct ChunkCursor {
            const arrow::ChunkedArray* col;
            int    chunk_idx  = 0;
            int64_t row_in_chunk = 0;

            const arrow::Array& current() const {
                return *col->chunk(chunk_idx);
            }
            void advance() {
                ++row_in_chunk;
                if (row_in_chunk >= col->chunk(chunk_idx)->length()) {
                    ++chunk_idx;
                    row_in_chunk = 0;
                }
            }
        };

        std::vector<ChunkCursor> cursors;
        cursors.reserve(show_cols);
        for (int ci = 0; ci < show_cols; ++ci)
            cursors.push_back({table->column(ci).get()});

        for (int64_t r = 0; r < rg_rows; ++r) {
            for (int ci = 0; ci < show_cols; ++ci) {
                if (ci) std::putchar(sep);
                auto& cur = cursors[ci];
                std::string val = cell_to_string(cur.current(), cur.row_in_chunk);
                // nulls → empty field (standard CSV convention)
                if (val != "null") write_csv_field(val, sep);
                cur.advance();
            }
            std::putchar('\n');
        }
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    Config cfg = parse_args(argc, argv);

    // Initialise colors
    bool use_color = (cfg.color == ColorMode::Always) ||
                     (cfg.color == ColorMode::Auto && isatty(STDOUT_FILENO));
    if (use_color) init_colors();

    // --- Open parquet file ---
    auto maybe_file = arrow::io::ReadableFile::Open(cfg.path);
    if (!maybe_file.ok()) {
        std::fprintf(stderr, "Error opening '%s': %s\n",
                     cfg.path.c_str(), maybe_file.status().ToString().c_str());
        return 1;
    }
    auto infile = maybe_file.ValueOrDie();

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

    // ── Delimited output mode ─────────────────────────────────────────────────
    if (cfg.delimiter) {
        // Default to all rows in delimiter mode (user can still pass -n)
        Config dcfg = cfg;
        if (dcfg.head_rows == 10) dcfg.head_rows = 0;   // 10 is the unchanged default
        write_delimited(arrow_reader.get(), schema, file_meta.get(), dcfg);
        return 0;
    }

    int show_cols = (cfg.max_cols > 0)
                    ? std::min(cfg.max_cols, (int)schema->num_fields())
                    : (int)schema->num_fields();

    int64_t rows_wanted = (cfg.head_rows <= 0) ? total_rows : (int64_t)cfg.head_rows;

    std::vector<int> col_indices;
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
        idx.is_index    = true;
        int digits = 1;
        for (int64_t v = std::max<int64_t>(n_display - 1, 0); v >= 10; v /= 10) ++digits;
        idx.width = digits;
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
        col.is_bool     = (display_type(*field) == arrow::Type::BOOL);
        col.width       = std::max(display_width(col.header), 4);

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

    // --- Draw table ---
    draw_separator(columns);

    {
        std::vector<std::string> hdr;
        std::vector<bool> ra;
        for (auto& c : columns) { hdr.push_back(c.header); ra.push_back(false); }
        draw_row(columns, hdr, ra, /*is_header=*/true);
    }
    draw_separator(columns);

    for (int64_t r = 0; r < n_display; ++r) {
        std::vector<std::string> row;
        std::vector<bool> ra;
        for (auto& c : columns) { row.push_back(c.cells[r]); ra.push_back(c.right_align); }
        draw_row(columns, row, ra);
    }
    draw_separator(columns);

    if (show_cols < num_cols)
        std::printf("  ... %d more column(s) not shown (-c 0 to see all)\n",
                    num_cols - show_cols);

    // --- Summary ---
    std::printf("\n%s[%lld rows x %d columns]%s\n",
                g_color.meta_key, (long long)total_rows, num_cols, g_color.reset);

    // Schema table
    int name_w = 6, type_w = 4;
    for (int ci = 0; ci < (int)schema->num_fields(); ++ci) {
        auto f = schema->field(ci);
        name_w = std::max(name_w, (int)f->name().size());
        type_w = std::max(type_w, (int)f->type()->ToString().size());
    }
    name_w = std::min(name_w, 40);
    type_w = std::min(type_w, 40);

    std::printf("\n%s%-*s  %-*s  Nullable%s\n",
                g_color.header, name_w, "Column", type_w, "Type", g_color.reset);
    std::printf("%s%s  %s  --------%s\n",
                g_color.border,
                std::string(name_w, '-').c_str(),
                std::string(type_w, '-').c_str(),
                g_color.reset);

    for (int ci = 0; ci < (int)schema->num_fields(); ++ci) {
        auto f = schema->field(ci);
        std::string fname = truncate(f->name(), name_w);
        std::string ftype = truncate(f->type()->ToString(), type_w);
        const char* tc    = *g_color.reset ? type_color(display_type(*f)) : "";
        std::printf("%-*s  %s%-*s%s  %s\n",
                    name_w, fname.c_str(),
                    tc, type_w, ftype.c_str(), g_color.reset,
                    f->nullable() ? "yes" : "no");
    }

    // File metadata
    int64_t total_size = 0;
    for (int rg = 0; rg < num_rg; ++rg)
        total_size += file_meta->RowGroup(rg)->total_compressed_size();

    char size_buf[32];
    if      (total_size < 1024)
        std::snprintf(size_buf, sizeof(size_buf), "%lld B", (long long)total_size);
    else if (total_size < 1024 * 1024)
        std::snprintf(size_buf, sizeof(size_buf), "%.1f KiB", total_size / 1024.0);
    else if (total_size < 1024LL * 1024 * 1024)
        std::snprintf(size_buf, sizeof(size_buf), "%.2f MiB", total_size / (1024.0 * 1024));
    else
        std::snprintf(size_buf, sizeof(size_buf), "%.2f GiB", total_size / (1024.0 * 1024 * 1024));

    std::printf("\n%sFile:%s %s\n",
                g_color.meta_key, g_color.reset, cfg.path.c_str());
    std::printf("%sRow groups:%s %d  %s|%s  %sCompressed size:%s %s\n",
                g_color.meta_key, g_color.reset, num_rg,
                g_color.border, g_color.reset,
                g_color.meta_key, g_color.reset, size_buf);
    if (!file_meta->created_by().empty())
        std::printf("%sCreated by:%s %s\n",
                    g_color.meta_key, g_color.reset, file_meta->created_by().c_str());

    return 0;
}
