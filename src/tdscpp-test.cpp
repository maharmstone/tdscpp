/* Copyright (c) Mark Harmstone 2024
 *
 * This file is part of tdscpp.
 *
 * tdscpp is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public Licence as published by
 * the Free Software Foundation, either version 3 of the Licence, or
 * (at your option) any later version.
 *
 * tdscpp is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public Licence for more details.
 *
 * You should have received a copy of the GNU Lesser General Public Licence
 * along with tdscpp.  If not, see <http://www.gnu.org/licenses/>. */

#include "tdscpp.h"
#include <iostream>

#ifdef _WIN32
#include <windows.h>
#else
#include <codecvt>
#endif

#ifdef __cpp_lib_format
#include <format>
#else
#include <fmt/format.h>
using fmt::format;
#endif

using namespace std;

static void show_msg(string_view, string_view message, string_view, int32_t msgno, int32_t, int16_t,
                     uint8_t severity, bool) {
    if (severity > 10)
        cout << format("\x1b[31;1mError {}: {}\x1b[0m\n", msgno, message);
    else if (msgno == 50000) // match SSMS by not displaying message no. if 50000 (RAISERROR etc.)
        cout << format("{}\n", message);
    else
        cout << format("{}: {}\n", msgno, message);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: tdscpp-test <server> [username] [password]\n");
        return 1;
    }

    try {
        string server = argv[1];
        string username = argc >= 3 ? argv[2] : "";
        string password = argc >= 4 ? argv[3] : "";

        // FIXME - prompt for password if username set but password isn't

        tds::tds n(server, username, password, "test program", "", show_msg);

#if 0
        cout << format("{}\n", (tds::datetime)tds::value("2020-10-29T01:23:45.0-03:00"));

        cout << format("{}\n", (tds::date)tds::value("2020-10-29"));
        cout << format("{}\n", (tds::date)tds::value("29/10/2020"));
        cout << format("{}\n", (tds::date)tds::value("29/10/20"));
        cout << format("{}\n", (tds::date)tds::value("29/Oct/2020"));
        cout << format("{}\n", (tds::date)tds::value("29/Oct/20"));
        cout << format("{}\n", (tds::date)tds::value("Oct 29, 2020"));
        cout << format("{}\n", (tds::date)tds::value("Oct 2020"));
        cout << format("{}\n", (tds::date)tds::value("2000-02-29"));

        cout << format("{}\n", (tds::time)tds::value("01:23:45.0"));
        cout << format("{}\n", (tds::time)tds::value("01:23:45"));
        cout << format("{}\n", (tds::time)tds::value("01:23"));
        cout << format("{}\n", (tds::time)tds::value("1AM"));
        cout << format("{}\n", (tds::time)tds::value("2 pm"));
        cout << format("{}\n", (tds::time)tds::value("2:56:34.0 pm"));
        cout << format("{}\n", (tds::time)tds::value("2:56:34 pm"));
        cout << format("{}\n", (tds::time)tds::value("2:56 pm"));
#endif
        {
            tds::query sq(n, "SELECT SYSTEM_USER AS [user], ? AS answer, ? AS greeting, ? AS now, ? AS pi, ? AS test", 42, "Hello", tds::datetimeoffset{2010y, chrono::October, 28d, 17, 58, 50, -360}, 3.1415926f, true);

            for (uint16_t i = 0; i < sq.num_columns(); i++) {
                cout << format("{}\t", tds::utf16_to_utf8(sq[i].name));
            }
            cout << format("\n");

            while (sq.fetch_row()) {
                for (uint16_t i = 0; i < sq.num_columns(); i++) {
                    cout << (string)sq[i] << "\t";
                }
                cout << format("\n");
            }
        }

        {
            tds::trans t(n);
            tds::trans t2(n);

            n.run("DROP TABLE IF EXISTS dbo.test2; CREATE TABLE dbo.test2(b VARCHAR(10));");

            t2.commit();
            t.commit();
        }

        {
            tds::batch b(n, u"SELECT SYSTEM_USER AS [user], 42 AS answer, @@TRANCOUNT AS tc ORDER BY 1");

            for (uint16_t i = 0; i < b.num_columns(); i++) {
                cout << format("{}\t", tds::utf16_to_utf8(b[i].name));
            }
            cout << format("\n");

            while (b.fetch_row()) {
                for (uint16_t i = 0; i < b.num_columns(); i++) {
                    cout << (string)b[i] << "\t";
                }
                cout << format("\n");
            }
        }

        {
            tds::query sq(n, "SELECT CONVERT(HIERARCHYID, '/10000000000.20000000000/40000000000.1000000000000/') AS hier, CONVERT(HIERARCHYID, '/10000.20000/40000.1000000/'), CONVERT(HIERARCHYID, '/1998.2001/2077.2101/'), CONVERT(HIERARCHYID, '/80.171/229.1066/'), CONVERT(HIERARCHYID, '/16.21/79/'), CONVERT(HIERARCHYID, '/8.9/10/'), CONVERT(HIERARCHYID, '/4.5/6/'), CONVERT(HIERARCHYID, '/1.2/'), CONVERT(HIERARCHYID, '/-7.-6/-5.-4/'), CONVERT(HIERARCHYID, '/-72.-69/-18.-14/'), CONVERT(HIERARCHYID, '/-3000.-2000/-1000.-100/'), CONVERT(HIERARCHYID, '/-10000.-20000/-40000.-1000000/'), CONVERT(HIERARCHYID, '/-10000000000.-20000000000/-40000000000.-1000000000000/')");

            while (sq.fetch_row()) {
                for (uint16_t i = 0; i < sq.num_columns(); i++) {
                    cout << (string)sq[i] << "\t";
                }
                cout << format("\n");
            }
        }

        n.run("RAISERROR('Hello, world!', 0, 1)");

        n.run("DROP TABLE IF EXISTS dbo.test;");
        n.run("CREATE TABLE dbo.test(a VARCHAR(10));");
        n.bcp(u"dbo.test", vector{u"a"}, vector<vector<tds::value>>{{"1"}, {true}, {nullptr}});
    } catch (const exception& e) {
        cerr << format("Exception: {}\n", e.what());
        return 1;
    }

    return 0;
}
