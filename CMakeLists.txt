cmake_minimum_required(VERSION 3.16)
project(parquet_viewer CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-O2 -march=x86-64 -ffunction-sections -fdata-sections)

# ── Build mimalloc as a static lib from source ───────────────────────────────
include(FetchContent)
FetchContent_Declare(mimalloc
    GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
    GIT_TAG        v2.1.9
    GIT_SHALLOW    TRUE
)
set(MI_BUILD_SHARED      OFF CACHE BOOL "" FORCE)
set(MI_BUILD_OBJECT      OFF CACHE BOOL "" FORCE)
set(MI_BUILD_TESTS       OFF CACHE BOOL "" FORCE)
set(MI_INSTALL_TOPLEVEL  OFF CACHE BOOL "" FORCE)
# Don't override malloc/free — Arrow uses mi_* prefixed API; libc.a has its own malloc
set(MI_OVERRIDE          OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(mimalloc)

# ── Collect include paths from Arrow/Parquet CMake packages (headers only) ───
find_package(Arrow   REQUIRED)
find_package(Parquet REQUIRED)

get_target_property(ARROW_INCLUDE_DIRS   Arrow::arrow_static   INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(PARQUET_INCLUDE_DIRS Parquet::parquet_static INTERFACE_INCLUDE_DIRECTORIES)

# ── Static link line via pkg-config ──────────────────────────────────────────
# pkg-config gives us the authoritative ordered set of -l flags; with -static
# the linker resolves each to a .a file without CMake targets meddling.
find_package(PkgConfig REQUIRED)
pkg_check_modules(PARQUET_PC REQUIRED IMPORTED_TARGET parquet)

# Extract raw -l list from pkg-config (--static expands transitive deps)
execute_process(
    COMMAND pkg-config --static --libs parquet
    OUTPUT_VARIABLE PKG_LINK_FLAGS
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
# Split into a CMake list
separate_arguments(PKG_LINK_FLAGS_LIST UNIX_COMMAND "${PKG_LINK_FLAGS}")

# ── Target ────────────────────────────────────────────────────────────────────
add_executable(parquet_viewer main.cpp)

target_include_directories(parquet_viewer PRIVATE
    ${ARROW_INCLUDE_DIRS}
    ${PARQUET_INCLUDE_DIRS}
)

# Use absolute .a paths for parquet + arrow so the linker finds them first,
# then the pkg-config -l flags (resolved to .a by -static) for everything else.
target_link_libraries(parquet_viewer PRIVATE
    /usr/lib/x86_64-linux-gnu/libparquet.a
    /usr/lib/x86_64-linux-gnu/libarrow.a
    ${PKG_LINK_FLAGS_LIST}   # transitive deps from pkg-config
    mimalloc-static          # built from source
)

# Fully static: libc + libm are linked from .a too — no glibc version req.
target_link_options(parquet_viewer PRIVATE
    -static
    -Wl,--gc-sections
)

# Strip in Release
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  add_custom_command(TARGET parquet_viewer POST_BUILD
      COMMAND ${CMAKE_STRIP} --strip-all $<TARGET_FILE:parquet_viewer>
      COMMENT "Stripping binary")
endif()

message(STATUS "Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "pkg-config : ${PKG_LINK_FLAGS}")
