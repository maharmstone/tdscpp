tdscpp
======

tdscpp is a C++ library for Microsoft's Tabluar Data Stream (TDS) protocol, which is used to communicate with Microsoft SQL Server. Using this library you can write programs which interface directly with MSSQL, without relying on ODBC or OLEDB. It requires C++20, and works on the latest versions of GCC, MSVC, and Clang.

Installation
------------

For vcpkg, run `vcpkg install tdscpp`.

For Gentoo, install the GURU repository and run `emerge tdscpp`.

For manual installation, it's much the same as for any CMake project:

````
mkdir build
cd build
cmake ..
make
make install
````

On Linux, to add Kerberos support add `-DENABLE_KRB5=ON` to `cmake`. To add encryption
support, add `-DWITH_OPENSSL=ON` to `cmake`.

On Windows, both Kerberos support and encryption are provided by the operating system,
so you don't need to do this. If you set `WITH_OPENSSL` OpenSSL will be linked in,
but otherwise Windows' own Schannel will be used instead.

Once installed, you can add it to your CMakeLists.txt:

````
set(CMAKE_CXX_STANDARD 20)
find_package(tdscpp REQUIRED)
target_link_libraries(project tdscpp)
````

Make sure that you're using at least C++20.

For GCC, just add `-ltdscpp`:
````
g++ -std=c++20 -ltdscpp test.cpp
````

Release history
---------------

* 20250301
    * Fixed for libfmt 11

* 20240707
    * Added support for older compiler versions
    * pkgconfig now used to find Kerberos on Linux
    * Fixed static compilation

* 20240629
    * Fixed compilation on 32-bit platforms
    * Fixed compilation on Clang
    * Fixed case where sp_prepare doesn't know columns
    * Fixed handling of short NUMERICs in BCP
    * Sped up reading large VARCHAR(MAX) values
    * Added CI

* 20240212
    * Initial release

Sample code
-----------

````cpp
#include <tdscpp.h>
#include <nlohmann/json.hpp>
#include <format>
#include <iostream>

int main() {
    try {
        /* Connect to server mssql with username "sa" and password "password". If you omit
           the credentials, Kerberos / Active Directory authentication will be used.
           You can also pass more options by using a tds::options struct instead. */
        tds::tds conn("mssql", "sa", "password");

        /* Issue commands by using the run method. These will throw an exception if the server
           returns an error. */
        conn.run("DROP TABLE IF EXISTS dbo.tmp");
        conn.run("CREATE TABLE dbo.tmp(num INT, str VARCHAR(10), str2 NVARCHAR(10), dt DATETIME2(0), not_set INT)");

        {
            /* Begin a transaction. This will automatically get rolled back if it goes out of
               scope without tran.commit() being called, such as if an exception is thrown. */
            tds::trans tran(conn);

            /* You can use question marks for placeholder variables. There's basic constexpr
               sanity checking, meaning that if you forget a variable or have mismatched brackets
               your code won't compile. There's also full Unicode support. */
            conn.run("INSERT INTO dbo.tmp(num, str, str2, dt) VALUES(42, ?, ?, GETDATE())",
                     "hello", u8"🔥");

            {
                /* Run a query. You can only have one active query at a time per connection,
                   so it's a good idea to wrap it in curly brackets so it gets released when
                   it goes out of scope. */
                tds::query sq(conn, "SELECT * FROM dbo.tmp WHERE num = ?", 42);

                /* fetch_row() returns false when there's no more results. */
                while (sq.fetch_row()) {
                    /* The returned data is in tds::value structs, which can be cast to int,
                       std::string, etc., or passed to std::format as below. */
                    std::cout << std::format("{}, {}, {}, {}", sq[0], sq[1], sq[2], sq[3]) << std::endl;

                    /* Or you can pass it to nlohmann::json. */
                    auto j = nlohmann::json::object();

                    for (unsigned int i = 0; i < sq.num_columns(); i++) {
                        /* MSSQL stores column names at UTF-16, but you can use tds::utf16_to_utf8
                           to fix this. */
                        j[tds::utf16_to_utf8(sq[i].name)] = sq[i];
                    }

                    std::cout << j.dump() << std::endl;
                }
            }

            /* Commit the transaction we opened before. */
            tran.commit();
        }
    } catch (const std::exception& e) {
        std::cerr << std::format("Exception: {}\n", e.what());
        return 1;
    }

    return 0;
}
````

Sample output:
````
42, hello, 🔥, 2024-02-12 21:21:05
{"dt":"2024-02-12 21:21:05","not_set":null,"num":42,"str":"hello","str2":"🔥"}
````

### Remote Procedure Calls (RPC)

You can call Remote Procedure Calls, either inbuilt ones or custom stored procedures,
by using `tds::rpc` in much the same way as you use `tds::query`.

````cpp
conn.run("DROP VIEW IF EXISTS dbo.test_view");
conn.run("CREATE VIEW dbo.test_view AS SELECT * FROM dbo.tmp");

{
    tds::rpc r(conn, "sp_depends", "dbo.test_view");

    while (r.fetch_row()) {
        for (size_t i = 0; i < r.num_columns(); i++) {
            std::cout << (std::string)r[i] << " (" << tds::utf16_to_utf8(r[i].name) << ")" << std::endl;
        }
        std::cout << "---" << std::endl;
    }
}
````

Output:
````
dbo.tmp (name)
user table (type)
no (updated)
yes (selected)
num (column)
---
...
````

### Bulk Copy Protocol (BCP)

You can use `tds::bcp` to load data into the database quickly. It takes three arguments:
the name of the table, an input range of the column names, and an input range of
an input range of the values. If you pass normal strings rather than UTF-16, they will
get converted for you.

`std::optional` can be used to represent nullable values.

````cpp
conn.bcp(u"dbo.tmp", std::vector{u"num", u"str", u"str2"}, std::vector<std::vector<tds::value>>{
    {28, "foo", "bar"},
    {29, "baz", std::optional<std::string>{std::nullopt}}
});
````

### Messages

The `tds::tds` constructor can also be passed a function pointer or a lambda to receive
any messages that are sent by the server, including error messages. The default behaviour
is to throw an exception if any messages are received with a severity more than 10.

````cpp
static void show_msg(std::string_view server, std::string_view message, std::string_view proc_name,
                     int32_t msgno, int32_t line_number, int16_t state, uint8_t severity,
                     bool error) {
    if (severity > 10) /* print errors in red */
        std::cout << std::format("\x1b[31;1mError {}: {}\x1b[0m\n", msgno, message);
    else if (msgno == 50000) /* match SSMS by not displaying message no. if 50000 (RAISERROR etc.) */
        std::cout << std::format("{}\n", message);
    else
        std::cout << std::format("{}: {}\n", msgno, message);
}

int main() {
    /* See the definition of tds::options for an exhaustive list of all
       the possible parameters here. */
    tds::tds conn("mssql", "sa", "password", "test program", "", show_msg);

    conn.run("RAISERROR('hello world', 10, 1)");
    conn.run("THROW 50000, ':-(', 1");

    return 0;
}
````

Output:
````
hello world
Error 50000: :-(
````

Data types
----------

The struct `tds::value` represents any value that might be stored in a MSSQL database. The spaceship
operator `<=>` is defined, meaning that you can compare values using (hopefully) the
exact same logic as on the server itself.

`tds::numeric<N>` can be used to represent NUMERIC(N) or DECIMAL(N) types, where
N is a number between 0 and 38.

`tds::datetime` can be used to represent DATETIME and DATETIME2 types. `tds::datetimeoffset`
adds an offset value, for DATETIMEOFFSET. DATE maps to `std::chrono::year_month_day`,
and TIME to `std::chrono::time_point`.

Compile-time checks
-------------------

If you pass a string to `tds::query` or `tds::rpc`, the library will do some basic sanity checking
at compile-time, and refuse to compile if these fail. These include making sure that there is the
same number of open brackets and close brackets, and that there are no unterminated quotation
marks.

If you want to pass a string that you have constructed in as a query, you will need to
wrap it in `tds::no_check`, e.g.:

````cpp
static void do_insert(tds::tds& tds, const std::string& column_name, std::string_view value) {
    std::string q;

    q = "INSERT INTO tbl(" + column + ") VALUES(?)";

    tds.run(tds::no_check{q}, value);

    ...
}
````

Note that you should be only doing this if you have to, and never for values. The safe way to
pass arbitrary values if to use the question mark notation as above. Inserting them directly
into the string leaves you vulnerable to SQL injection.
