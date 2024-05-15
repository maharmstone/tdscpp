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

#pragma once

#include <string>
#include <cstdint>
#include <thread>
#include <condition_variable>
#include <format>
#include "config.h"

#ifdef _WIN32
#define SECURITY_WIN32
#include <windows.h>
#include <sspi.h>
#endif

#ifdef WITH_OPENSSL
#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/conf.h>
#include <openssl/x509v3.h>
#endif

class formatted_error : public std::exception {
public:
    template<typename... Args>
    formatted_error(std::format_string<Args...> s, Args&&... args) : msg(std::format(s, std::forward<Args>(args)...)) {
    }

    const char* what() const noexcept {
        return msg.c_str();
    }

private:
    std::string msg;
};

enum class tds_msg : uint8_t {
    sql_batch = 1,
    pretds7_login,
    rpc,
    tabular_result,
    attention_signal = 6,
    bulk_load_data,
    federated_auth_token,
    trans_man_req = 14,
    tds7_login = 16,
    sspi,
    prelogin
};

struct tds_header {
    enum tds_msg type;
    uint8_t status;
    uint16_t length;
    uint16_t spid;
    uint8_t packet_id;
    uint8_t window;
};

static_assert(sizeof(tds_header) == 8, "tds_header has wrong size");

enum class tds_login_opt_type : uint8_t {
    version = 0,
    encryption,
    instopt,
    threadid,
    mars,
    traceid,
    fedauthrequired,
    nonceopt,
    terminator = 0xff
};

struct login_opt {
    login_opt(enum tds_login_opt_type type, std::span<const uint8_t> payload) : type(type), payload(payload.data(), payload.data() + payload.size()) { }

    template<typename T>
    requires std::is_integral_v<T> || std::is_enum_v<T>
    login_opt(enum tds_login_opt_type type, T payload) : login_opt(type, std::span{(const uint8_t*)&payload, sizeof(payload)}) { }

    enum tds_login_opt_type type;
    std::vector<uint8_t> payload;
};

struct login_opt_version {
    uint8_t major;
    uint8_t minor;
    uint16_t build;
    uint16_t subbuild;
};

static_assert(sizeof(login_opt_version) == 6, "login_opt_version has wrong size");

#pragma pack(push,1)

struct tds_login_opt {
    enum tds_login_opt_type type;
    uint16_t offset;
    uint16_t length;
};

#pragma pack(pop)

static_assert(sizeof(tds_login_opt) == 5, "tds_login_opt has wrong size");

#pragma pack(push,1)

struct tds_login_msg {
    uint32_t length;
    uint32_t tds_version;
    uint32_t packet_size;
    uint32_t client_version;
    uint32_t client_pid;
    uint32_t connexion_id;
    uint8_t option_flags1;
    uint8_t option_flags2;
    struct {
        uint8_t sql_type : 4;
        uint8_t oledb : 1;
        uint8_t read_only_intent : 1;
        uint8_t reserved : 2;
    } sql_type_flags;
    uint8_t option_flags3;
    int32_t timezone;
    uint32_t collation;
    uint16_t client_name_offset;
    uint16_t client_name_length;
    uint16_t username_offset;
    uint16_t username_length;
    uint16_t password_offset;
    uint16_t password_length;
    uint16_t app_name_offset;
    uint16_t app_name_length;
    uint16_t server_name_offset;
    uint16_t server_name_length;
    uint16_t extension_offset;
    uint16_t extension_length;
    uint16_t interface_library_offset;
    uint16_t interface_library_length;
    uint16_t locale_offset;
    uint16_t locale_length;
    uint16_t database_offset;
    uint16_t database_length;
    uint8_t mac_address[6];
    uint16_t sspi_offset;
    uint16_t sspi_length;
    uint16_t attach_db_offset;
    uint16_t attach_db_length;
    uint16_t new_password_offset;
    uint16_t new_password_length;
    uint32_t sspi_long;
};

#pragma pack(pop)

static_assert(sizeof(tds_login_msg) == 94, "tds_login_msg has wrong size");

#pragma pack(push,1)

struct tds_done_msg {
    uint16_t status;
    uint16_t curcmd;
    uint64_t rowcount;
};

static_assert(sizeof(tds_done_msg) == 12, "tds_done_msg has wrong size");

struct tds_header_trans_desc {
    uint16_t type;
    uint64_t descriptor;
    uint32_t outstanding;
};

static_assert(sizeof(tds_header_trans_desc) == 14, "tds_header_trans_desc has wrong size");

struct tds_all_headers {
    uint32_t total_size;
    uint32_t size;
    tds_header_trans_desc trans_desc;
};

static_assert(sizeof(tds_all_headers) == 22, "tds_all_headers has wrong size");

struct tds_rpc_batch {
    tds_all_headers all_headers;
    uint16_t proc_id_switch;
    uint16_t proc_id;
    uint16_t flags;
};

static_assert(sizeof(tds_rpc_batch) == 28, "tds_rpc_batch has wrong size");

namespace tds {
    enum class sql_type : uint8_t;
}

struct tds_param_header {
    uint8_t name_len;
    uint8_t flags;
    tds::sql_type type;
};

struct tds_INT_param {
    tds_param_header h;
    uint8_t max_length;
    uint8_t length;
};

static_assert(sizeof(tds_INT_param) == 5, "tds_INT_param has wrong size");

struct tds_VARCHAR_param {
    tds_param_header h;
    uint16_t max_length;
    tds::collation collation;
    uint16_t length;
};

static_assert(sizeof(tds_VARCHAR_param) == 12, "tds_VARCHAR_param has wrong size");

struct tds_VARCHAR_MAX_param {
    tds_param_header h;
    uint16_t max_length;
    tds::collation collation;
    uint64_t length;
    uint32_t chunk_length;
};

static_assert(sizeof(tds_VARCHAR_MAX_param) == 22, "tds_VARCHAR_MAX_param has wrong size");

struct tds_VARBINARY_param {
    tds_param_header h;
    uint16_t max_length;
    uint16_t length;
};

static_assert(sizeof(tds_VARBINARY_param) == 7, "tds_VARBINARY_param has wrong size");

struct tds_VARBINARY_MAX_param {
    tds_param_header h;
    uint16_t max_length;
    uint64_t length;
    uint32_t chunk_length;
};

static_assert(sizeof(tds_VARBINARY_MAX_param) == 17, "tds_VARBINARY_MAX_param has wrong size");

struct tds_XML_param {
    tds_param_header h;
    uint8_t flags;
    uint64_t length;
    uint32_t chunk_length;
};

static_assert(sizeof(tds_XML_param) == 16, "tds_XML_param has wrong size");

struct tds_return_value {
    uint16_t param_ordinal;
    uint8_t param_name_len;
    // FIXME - then param name if present
    uint8_t status;
    uint32_t user_type;
    uint16_t flags;
    tds::sql_type type;
};

static_assert(sizeof(tds_return_value) == 11, "tds_return_value has wrong size");

enum class tds_tm_type : uint16_t {
    TM_GET_DTC_ADDRESS = 0,
    TM_PROPAGATE_XACT = 1,
    TM_BEGIN_XACT = 5,
    TM_PROMOTE_XACT = 6,
    TM_COMMIT_XACT = 7,
    TM_ROLLBACK_XACT = 8,
    TM_SAVE_XACT = 9
};

struct tds_tm_msg {
    tds_all_headers all_headers;
    enum tds_tm_type type;
};

static_assert(sizeof(tds_tm_msg) == 24, "tds_tm_msg has wrong size");

struct tds_tm_begin {
    tds_tm_msg header;
    uint8_t isolation_level;
    uint8_t name_len;
};

static_assert(sizeof(tds_tm_begin) == 26, "tds_tm_begin has wrong size");

struct tds_tm_rollback {
    tds_tm_msg header;
    uint8_t name_len;
    uint8_t flags;
};

static_assert(sizeof(tds_tm_rollback) == 26, "tds_tm_rollback has wrong size");

struct tds_tm_commit {
    tds_tm_msg header;
    uint8_t name_len;
    uint8_t flags;
};

static_assert(sizeof(tds_tm_commit) == 26, "tds_tm_commit has wrong size");

enum class tds_envchange_type : uint8_t {
    database = 1,
    language,
    charset,
    packet_size,
    unicode_data_sort_local_id,
    unicode_data_sort_comparison_flags,
    collation,
    begin_trans,
    commit_trans,
    rollback_trans,
    enlist_dist_trans,
    defect_trans,
    log_shipping,
    promote_trans = 15,
    trans_man_address,
    trans_ended,
    reset_completion_acknowledgement,
    user_instance_started,
    routing
};

struct tds_envchange {
    enum tds::token token;
    uint16_t length;
    enum tds_envchange_type type;
};

static_assert(sizeof(tds_envchange) == 4, "tds_envchange has wrong size");

struct tds_envchange_begin_trans {
    struct tds_envchange header;
    uint8_t new_len;
    uint64_t trans_id;
    uint8_t old_len;
};

static_assert(sizeof(tds_envchange_begin_trans) == 14, "tds_envchange_begin_trans has wrong size");

struct tds_envchange_rollback_trans {
    struct tds_envchange header;
    uint8_t new_len;
    uint8_t old_len;
    uint64_t trans_id;
};

static_assert(sizeof(tds_envchange_rollback_trans) == 14, "tds_envchange_rollback_trans has wrong size");

struct tds_envchange_commit_trans {
    struct tds_envchange header;
    uint8_t new_len;
    uint8_t old_len;
    uint64_t trans_id;
};

static_assert(sizeof(tds_envchange_commit_trans) == 14, "tds_envchange_commit_trans has wrong size");

struct tds_envchange_packet_size {
    struct tds_envchange header;
    uint8_t new_len;
};

static_assert(sizeof(tds_envchange_packet_size) == 5, "tds_envchange_packet_size has wrong size");

struct tds_envchange_database {
    struct tds_envchange header;
    uint8_t name_len;
};

static_assert(sizeof(tds_envchange_database) == 5, "tds_envchange_database has wrong size");

struct tds_envchange_collation {
    struct tds_envchange header;
    uint8_t new_len;
    tds::collation collation;
};

static_assert(sizeof(tds_envchange_collation) == 10, "tds_envchange_collation has wrong size");

struct tds_info_msg {
    int32_t msgno;
    uint8_t state;
    uint8_t severity;
};

static_assert(sizeof(tds_info_msg) == 6, "tds_info_msg has wrong size");

enum class tds_feature : uint8_t {
    SESSIONRECOVERY = 0x1,
    FEDAUTH = 0x2,
    COLUMNENCRYPTION = 0x4,
    GLOBALTRANSACTIONS = 0x5,
    AZURESQLSUPPORT = 0x8,
    DATACLASSIFICATION = 0x9,
    UTF8_SUPPORT = 0xa,
    AZURESQLDNSCACHING = 0xb,
    TERMINATOR = 0xff
};

enum class smp_message_type : uint8_t {
    SYN = 0x01,
    ACK = 0x02,
    FIN = 0x04,
    DATA = 0x08
};

struct smp_header {
    uint8_t smid;
    smp_message_type flags;
    uint16_t sid;
    uint32_t length;
    uint32_t seqnum;
    uint32_t wndw;
};

static_assert(sizeof(smp_header) == 16, "smp_header has wrong size");

#pragma pack(pop)

#ifdef _WIN32

class handle_closer {
public:
    typedef HANDLE pointer;

    void operator()(HANDLE h) {
        if (h == INVALID_HANDLE_VALUE)
            return;

        CloseHandle(h);
    }
};

using unique_handle = std::unique_ptr<HANDLE, handle_closer>;

#else

class unique_handle {
public:
    unique_handle() : fd(0) {
    }

    explicit unique_handle(int fd) : fd(fd) {
    }

    unique_handle(unique_handle&& that) noexcept {
        fd = that.fd;
        that.fd = 0;
    }

    unique_handle(const unique_handle&) = delete;
    unique_handle& operator=(const unique_handle&) = delete;

    unique_handle& operator=(unique_handle&& that) noexcept {
        if (fd > 0)
            close(fd);

        fd = that.fd;
        that.fd = 0;

        return *this;
    }

    ~unique_handle() {
        if (fd <= 0)
            return;

        close(fd);
    }

    explicit operator bool() const noexcept {
        return fd != 0;
    }

    void reset(int new_fd = 0) noexcept {
        if (fd > 0)
            close(fd);

        fd = new_fd;
    }

    int get() const noexcept {
        return fd;
    }

private:
    int fd;
};

#endif

#ifdef _WIN32
class last_error : public std::exception {
public:
    last_error(std::string_view function, int le) {
        std::string nice_msg;

        {
            char16_t* fm;

            if (FormatMessageW(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr,
                            le, 0, reinterpret_cast<LPWSTR>(&fm), 0, nullptr)) {
                try {
                    std::u16string_view s = fm;

                    while (!s.empty() && (s[s.length() - 1] == u'\r' || s[s.length() - 1] == u'\n')) {
                        s.remove_suffix(1);
                    }

                    nice_msg = tds::utf16_to_utf8(s);
                } catch (...) {
                    LocalFree(fm);
                    throw;
                }

                LocalFree(fm);
            }
        }

        msg = std::string(function) + " failed (error " + std::to_string(le) + (!nice_msg.empty() ? (", " + nice_msg) : "") + ").";
    }

    const char* what() const noexcept {
        return msg.c_str();
    }

private:
    std::string msg;
};

#endif

#ifdef WITH_OPENSSL
class bio_meth_deleter {
public:
    typedef BIO_METHOD* pointer;

    void operator()(BIO_METHOD* meth) {
        BIO_meth_free(meth);
    }
};

class ssl_deleter {
public:
    typedef SSL* pointer;

    void operator()(SSL* ssl) {
        SSL_free(ssl);
    }
};

class ssl_ctx_deleter {
public:
    typedef SSL_CTX* pointer;

    void operator()(SSL_CTX* ctx) {
        SSL_CTX_free(ctx);
    }
};
#endif

class event {
public:
    event();
    void reset();
    void set();

    unique_handle h;
};

class ringbuf;

namespace tds {
#if defined(WITH_OPENSSL) || defined(_WIN32)
    class tds_ssl;
#endif

    struct mess {
        enum tds_msg type;
        std::vector<uint8_t> payload;
        bool last_packet;
    };

    class tds_impl;

    template<typename T>
    concept sendable = requires(T t) {
        { t.send_msg((enum tds_msg)0, std::span<const uint8_t>()) };
    };

    class main_session {
    public:
        main_session(tds_impl& tds) : tds(tds) { }

        void wait_for_msg(enum tds_msg& type, std::vector<uint8_t>& payload, bool* last_packet = nullptr);
#if defined(WITH_OPENSSL) || defined(_WIN32)
        void send_msg(enum tds_msg type, std::span<const uint8_t> msg, bool do_ssl = true);
        void send_raw(std::span<const uint8_t> msg, bool do_ssl = true);
#else
        void send_msg(enum tds_msg type, std::span<const uint8_t> msg);
        void send_raw(std::span<const uint8_t> msg);
#endif

        tds_impl& tds;
        std::condition_variable mess_in_cv;
        std::mutex mess_in_lock;
        std::list<mess> mess_list;
        std::condition_variable_any rate_limit_cv;
        std::exception_ptr socket_thread_exc;
    };

    static_assert(sendable<main_session>);

    class tds_impl {
    public:
        tds_impl(const std::string& server, std::string_view user, std::string_view password,
                 std::string_view app_name, std::string_view db,
                 const msg_handler& message_handler,
                 const func_count_handler& count_handler, uint16_t port, encryption_type enc,
                 bool check_certificate, bool mars, unsigned int rate_limit,
                 bool read_only_intent);
        ~tds_impl();
        void handle_info_msg(std::span<const uint8_t> sp, bool error) const;

        template<typename... Args>
        void run(std::string_view s, Args&&... args);

        void bcp(std::u16string_view table, const std::vector<std::u16string>& np, const std::vector<std::vector<value>>& vp,
                 std::u16string_view db);

        void connect(const std::string& server, uint16_t port, bool get_fqdn);
        void send_prelogin_msg(enum encryption_type encrypt, bool do_mars);
        void send_login_msg(std::string_view user, std::string_view password, std::string_view server,
                            std::string_view app_name, std::string_view db, bool read_only_intent);
        void socket_thread(std::stop_token stop);
        void socket_thread_wrap(std::stop_token stop) noexcept;
        void socket_thread_read(ringbuf& in_buf);
        bool socket_thread_write();
        void socket_thread_parse_messages(std::stop_token stop, ringbuf& in_buf);
#if defined(WITH_OPENSSL) || defined(_WIN32)
        void decrypt_messages(ringbuf& in_buf, ringbuf& pt_buf);
#endif
#ifdef _WIN32
        void pipe_write();
#endif
        void handle_envchange_msg(std::span<const uint8_t> sp);

#ifdef _WIN32
        SOCKET sock = INVALID_SOCKET;
        unique_handle pipe{INVALID_HANDLE_VALUE};
#else
        int sock = 0;
#endif
        std::string fqdn, hostname;
        msg_handler message_handler;
        func_count_handler count_handler;
        uint32_t packet_size = 4096;
        uint16_t spid = 0;
        bool has_utf8 = false;
#if defined(WITH_OPENSSL) || defined(_WIN32)
        std::unique_ptr<tds_ssl> ssl;
#endif
        encryption_type server_enc = encryption_type::ENCRYPT_NOT_SUP;
        bool check_certificate;
        std::u16string db_name;
        collation coll{};
        uint64_t trans_id = 0;
        bool mars = false;
        std::mutex mars_lock;
        std::list<std::reference_wrapper<smp_session>> mars_list;
        std::unique_ptr<smp_session> mars_sess;
        uint16_t last_sid = 0;
        event mess_event;
        main_session sess{*this};
        std::mutex mess_out_lock;
        std::vector<uint8_t> mess_out_buf;
        unsigned int rate_limit;
        bool connected = true;
        std::jthread t;
    };

#if defined(WITH_OPENSSL) || defined(_WIN32)
    class tds_ssl {
    public:
        tds_ssl(tds_impl& tds);
#ifdef WITH_OPENSSL
        int ssl_read_cb(std::span<uint8_t> sp);
        int ssl_write_cb(std::span<const uint8_t> sp);
        long ssl_ctrl_cb(int cmd, long num, void* ptr);
        int ssl_verify_cb(int preverify, X509_STORE_CTX* x509_ctx);
#else
        ~tds_ssl();
#endif
        [[nodiscard]] std::vector<uint8_t> enc(std::span<const uint8_t> sp);
        [[nodiscard]] std::vector<uint8_t> dec(ringbuf& in_buf);

        std::exception_ptr exception;

    private:
        tds_impl& tds;
        std::vector<uint8_t> ssl_recv_buf;
        std::vector<uint8_t> ssl_send_buf;
        std::optional<std::reference_wrapper<ringbuf>> ssl_recv_rb;
#ifdef WITH_OPENSSL
        bool established = false;
        BIO* bio;
        std::unique_ptr<SSL_CTX*, ssl_ctx_deleter> ctx;
        std::unique_ptr<BIO_METHOD*, bio_meth_deleter> meth;
        std::unique_ptr<SSL*, ssl_deleter> ssl;
#else
        CredHandle cred_handle = {(ULONG_PTR)-1, (ULONG_PTR)-1};
        CtxtHandle ctx_handle;
        bool ctx_handle_set = false;
        SecPkgContext_StreamSizes stream_sizes;
#endif
    };
#endif

    class batch_impl {
    public:
        batch_impl(tds& conn, std::u16string_view q);
        batch_impl(session& sess, std::u16string_view q);
        ~batch_impl();

        bool fetch_row();
        void wait_for_packet();

        std::vector<column> cols;
        bool finished = false, received_attn = false;
        std::list<std::vector<std::pair<value_data_t, bool>>> rows;
        tds& conn;
        std::optional<std::reference_wrapper<smp_session>> sess;
        std::list<std::vector<uint8_t>> tokens;
        std::vector<uint8_t> buf;
        std::vector<column> buf_columns;
    };

    class smp_session {
    public:
        smp_session(tds_impl& tds);
        ~smp_session();
        void send_msg(enum tds_msg type, std::span<const uint8_t> msg);
        void wait_for_msg(enum tds_msg& type, std::vector<uint8_t>& payload, bool* last_packet = nullptr);
        void parse_message(std::stop_token stop, std::span<const uint8_t> msg);
        void send_ack();

        tds_impl& tds;
        uint32_t seqnum = 1;
        uint16_t sid;
        std::condition_variable mess_in_cv;
        std::mutex mess_in_lock;
        std::list<mess> mess_list;
        std::condition_variable_any rate_limit_cv;
        std::exception_ptr socket_thread_exc;
        uint32_t recv_wndw;
    };

    static_assert(sendable<smp_session>);
};

#ifdef _WIN32
enum class sec_error : uint32_t {
    _SEC_E_OK = 0,
    _SEC_E_INSUFFICIENT_MEMORY = 0x80090300,
    _SEC_E_INVALID_HANDLE = 0x80090301,
    _SEC_E_UNSUPPORTED_FUNCTION = 0x80090302,
    _SEC_E_TARGET_UNKNOWN = 0x80090303,
    _SEC_E_INTERNAL_ERROR = 0x80090304,
    _SEC_E_SECPKG_NOT_FOUND = 0x80090305,
    _SEC_E_NOT_OWNER = 0x80090306,
    _SEC_E_CANNOT_INSTALL = 0x80090307,
    _SEC_E_INVALID_TOKEN = 0x80090308,
    _SEC_E_CANNOT_PACK = 0x80090309,
    _SEC_E_QOP_NOT_SUPPORTED = 0x8009030A,
    _SEC_E_NO_IMPERSONATION = 0x8009030B,
    _SEC_E_LOGON_DENIED = 0x8009030C,
    _SEC_E_UNKNOWN_CREDENTIALS = 0x8009030D,
    _SEC_E_NO_CREDENTIALS = 0x8009030E,
    _SEC_E_MESSAGE_ALTERED = 0x8009030F,
    _SEC_E_OUT_OF_SEQUENCE = 0x80090310,
    _SEC_E_NO_AUTHENTICATING_AUTHORITY = 0x80090311,
    _SEC_I_CONTINUE_NEEDED = 0x00090312,
    _SEC_I_COMPLETE_NEEDED = 0x00090313,
    _SEC_I_COMPLETE_AND_CONTINUE = 0x00090314,
    _SEC_I_LOCAL_LOGON = 0x00090315,
    _SEC_I_GENERIC_EXTENSION_RECEIVED = 0x00090316,
    _SEC_E_BAD_PKGID = 0x80090316,
    _SEC_E_CONTEXT_EXPIRED = 0x80090317,
    _SEC_I_CONTEXT_EXPIRED = 0x00090317,
    _SEC_E_INCOMPLETE_MESSAGE = 0x80090318,
    _SEC_E_INCOMPLETE_CREDENTIALS = 0x80090320,
    _SEC_E_BUFFER_TOO_SMALL = 0x80090321,
    _SEC_I_INCOMPLETE_CREDENTIALS = 0x00090320,
    _SEC_I_RENEGOTIATE = 0x00090321,
    _SEC_E_WRONG_PRINCIPAL = 0x80090322,
    _SEC_I_NO_LSA_CONTEXT = 0x00090323,
    _SEC_E_TIME_SKEW = 0x80090324,
    _SEC_E_UNTRUSTED_ROOT = 0x80090325,
    _SEC_E_ILLEGAL_MESSAGE = 0x80090326,
    _SEC_E_CERT_UNKNOWN = 0x80090327,
    _SEC_E_CERT_EXPIRED = 0x80090328,
    _SEC_E_ENCRYPT_FAILURE = 0x80090329,
    _SEC_E_DECRYPT_FAILURE = 0x80090330,
    _SEC_E_ALGORITHM_MISMATCH = 0x80090331,
    _SEC_E_SECURITY_QOS_FAILED = 0x80090332,
    _SEC_E_UNFINISHED_CONTEXT_DELETED = 0x80090333,
    _SEC_E_NO_TGT_REPLY = 0x80090334,
    _SEC_E_NO_IP_ADDRESSES = 0x80090335,
    _SEC_E_WRONG_CREDENTIAL_HANDLE = 0x80090336,
    _SEC_E_CRYPTO_SYSTEM_INVALID = 0x80090337,
    _SEC_E_MAX_REFERRALS_EXCEEDED = 0x80090338,
    _SEC_E_MUST_BE_KDC = 0x80090339,
    _SEC_E_STRONG_CRYPTO_NOT_SUPPORTED = 0x8009033A,
    _SEC_E_TOO_MANY_PRINCIPALS = 0x8009033B,
    _SEC_E_NO_PA_DATA = 0x8009033C,
    _SEC_E_PKINIT_NAME_MISMATCH = 0x8009033D,
    _SEC_E_SMARTCARD_LOGON_REQUIRED = 0x8009033E,
    _SEC_E_SHUTDOWN_IN_PROGRESS = 0x8009033F,
    _SEC_E_KDC_INVALID_REQUEST = 0x80090340,
    _SEC_E_KDC_UNABLE_TO_REFER = 0x80090341,
    _SEC_E_KDC_UNKNOWN_ETYPE = 0x80090342,
    _SEC_E_UNSUPPORTED_PREAUTH = 0x80090343,
    _SEC_E_DELEGATION_REQUIRED = 0x80090345,
    _SEC_E_BAD_BINDINGS = 0x80090346,
    _SEC_E_MULTIPLE_ACCOUNTS = 0x80090347,
    _SEC_E_NO_KERB_KEY = 0x80090348,
    _SEC_E_CERT_WRONG_USAGE = 0x80090349,
    _SEC_E_DOWNGRADE_DETECTED = 0x80090350,
    _SEC_E_SMARTCARD_CERT_REVOKED = 0x80090351,
    _SEC_E_ISSUING_CA_UNTRUSTED = 0x80090352,
    _SEC_E_REVOCATION_OFFLINE_C = 0x80090353,
    _SEC_E_PKINIT_CLIENT_FAILURE = 0x80090354,
    _SEC_E_SMARTCARD_CERT_EXPIRED = 0x80090355,
    _SEC_E_NO_S4U_PROT_SUPPORT = 0x80090356,
    _SEC_E_CROSSREALM_DELEGATION_FAILURE = 0x80090357,
    _SEC_E_REVOCATION_OFFLINE_KDC = 0x80090358,
    _SEC_E_ISSUING_CA_UNTRUSTED_KDC = 0x80090359,
    _SEC_E_KDC_CERT_EXPIRED = 0x8009035A,
    _SEC_E_KDC_CERT_REVOKED = 0x8009035B,
    _SEC_I_SIGNATURE_NEEDED = 0x0009035C,
    _SEC_E_INVALID_PARAMETER = 0x8009035D,
    _SEC_E_DELEGATION_POLICY = 0x8009035E,
    _SEC_E_POLICY_NLTM_ONLY = 0x8009035F,
    _SEC_I_NO_RENEGOTIATION = 0x00090360,
    _SEC_E_NO_CONTEXT = 0x80090361,
    _SEC_E_PKU2U_CERT_FAILURE = 0x80090362,
    _SEC_E_MUTUAL_AUTH_FAILED = 0x80090363,
    _SEC_I_MESSAGE_FRAGMENT = 0x00090364,
    _SEC_E_ONLY_HTTPS_ALLOWED = 0x80090365,
    _SEC_I_CONTINUE_NEEDED_MESSAGE_OK = 0x00090366,
    _SEC_E_APPLICATION_PROTOCOL_MISMATCH = 0x80090367,
    _SEC_I_ASYNC_CALL_PENDING = 0x00090368,
    _SEC_E_INVALID_UPN_NAME = 0x80090369,
    _SEC_E_EXT_BUFFER_TOO_SMALL = 0x8009036A,
    _SEC_E_INSUFFICIENT_BUFFERS = 0x8009036B
};

template<>
struct std::formatter<enum sec_error> {
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();

        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template<typename format_context>
    auto format(enum sec_error t, format_context& ctx) const {
        switch (t) {
            case sec_error::_SEC_E_OK:
                return std::format_to(ctx.out(), "SEC_E_OK");

            case sec_error::_SEC_E_INSUFFICIENT_MEMORY:
                return std::format_to(ctx.out(), "SEC_E_INSUFFICIENT_MEMORY");

            case sec_error::_SEC_E_INVALID_HANDLE:
                return std::format_to(ctx.out(), "SEC_E_INVALID_HANDLE");

            case sec_error::_SEC_E_UNSUPPORTED_FUNCTION:
                return std::format_to(ctx.out(), "SEC_E_UNSUPPORTED_FUNCTION");

            case sec_error::_SEC_E_TARGET_UNKNOWN:
                return std::format_to(ctx.out(), "SEC_E_TARGET_UNKNOWN");

            case sec_error::_SEC_E_INTERNAL_ERROR:
                return std::format_to(ctx.out(), "SEC_E_INTERNAL_ERROR");

            case sec_error::_SEC_E_SECPKG_NOT_FOUND:
                return std::format_to(ctx.out(), "SEC_E_SECPKG_NOT_FOUND");

            case sec_error::_SEC_E_NOT_OWNER:
                return std::format_to(ctx.out(), "SEC_E_NOT_OWNER");

            case sec_error::_SEC_E_CANNOT_INSTALL:
                return std::format_to(ctx.out(), "SEC_E_CANNOT_INSTALL");

            case sec_error::_SEC_E_INVALID_TOKEN:
                return std::format_to(ctx.out(), "SEC_E_INVALID_TOKEN");

            case sec_error::_SEC_E_CANNOT_PACK:
                return std::format_to(ctx.out(), "SEC_E_CANNOT_PACK");

            case sec_error::_SEC_E_QOP_NOT_SUPPORTED:
                return std::format_to(ctx.out(), "SEC_E_QOP_NOT_SUPPORTED");

            case sec_error::_SEC_E_NO_IMPERSONATION:
                return std::format_to(ctx.out(), "SEC_E_NO_IMPERSONATION");

            case sec_error::_SEC_E_LOGON_DENIED:
                return std::format_to(ctx.out(), "SEC_E_LOGON_DENIED");

            case sec_error::_SEC_E_UNKNOWN_CREDENTIALS:
                return std::format_to(ctx.out(), "SEC_E_UNKNOWN_CREDENTIALS");

            case sec_error::_SEC_E_NO_CREDENTIALS:
                return std::format_to(ctx.out(), "SEC_E_NO_CREDENTIALS");

            case sec_error::_SEC_E_MESSAGE_ALTERED:
                return std::format_to(ctx.out(), "SEC_E_MESSAGE_ALTERED");

            case sec_error::_SEC_E_OUT_OF_SEQUENCE:
                return std::format_to(ctx.out(), "SEC_E_OUT_OF_SEQUENCE");

            case sec_error::_SEC_E_NO_AUTHENTICATING_AUTHORITY:
                return std::format_to(ctx.out(), "SEC_E_NO_AUTHENTICATING_AUTHORITY");

            case sec_error::_SEC_I_CONTINUE_NEEDED:
                return std::format_to(ctx.out(), "SEC_I_CONTINUE_NEEDED");

            case sec_error::_SEC_I_COMPLETE_NEEDED:
                return std::format_to(ctx.out(), "SEC_I_COMPLETE_NEEDED");

            case sec_error::_SEC_I_COMPLETE_AND_CONTINUE:
                return std::format_to(ctx.out(), "SEC_I_COMPLETE_AND_CONTINUE");

            case sec_error::_SEC_I_LOCAL_LOGON:
                return std::format_to(ctx.out(), "SEC_I_LOCAL_LOGON");

            case sec_error::_SEC_I_GENERIC_EXTENSION_RECEIVED:
                return std::format_to(ctx.out(), "SEC_I_GENERIC_EXTENSION_RECEIVED");

            case sec_error::_SEC_E_BAD_PKGID:
                return std::format_to(ctx.out(), "SEC_E_BAD_PKGID");

            case sec_error::_SEC_E_CONTEXT_EXPIRED:
                return std::format_to(ctx.out(), "SEC_E_CONTEXT_EXPIRED");

            case sec_error::_SEC_I_CONTEXT_EXPIRED:
                return std::format_to(ctx.out(), "SEC_I_CONTEXT_EXPIRED");

            case sec_error::_SEC_E_INCOMPLETE_MESSAGE:
                return std::format_to(ctx.out(), "SEC_E_INCOMPLETE_MESSAGE");

            case sec_error::_SEC_E_INCOMPLETE_CREDENTIALS:
                return std::format_to(ctx.out(), "SEC_E_INCOMPLETE_CREDENTIALS");

            case sec_error::_SEC_E_BUFFER_TOO_SMALL:
                return std::format_to(ctx.out(), "SEC_E_BUFFER_TOO_SMALL");

            case sec_error::_SEC_I_INCOMPLETE_CREDENTIALS:
                return std::format_to(ctx.out(), "SEC_I_INCOMPLETE_CREDENTIALS");

            case sec_error::_SEC_I_RENEGOTIATE:
                return std::format_to(ctx.out(), "SEC_I_RENEGOTIATE");

            case sec_error::_SEC_E_WRONG_PRINCIPAL:
                return std::format_to(ctx.out(), "SEC_E_WRONG_PRINCIPAL");

            case sec_error::_SEC_I_NO_LSA_CONTEXT:
                return std::format_to(ctx.out(), "SEC_I_NO_LSA_CONTEXT");

            case sec_error::_SEC_E_TIME_SKEW:
                return std::format_to(ctx.out(), "SEC_E_TIME_SKEW");

            case sec_error::_SEC_E_UNTRUSTED_ROOT:
                return std::format_to(ctx.out(), "SEC_E_UNTRUSTED_ROOT");

            case sec_error::_SEC_E_ILLEGAL_MESSAGE:
                return std::format_to(ctx.out(), "SEC_E_ILLEGAL_MESSAGE");

            case sec_error::_SEC_E_CERT_UNKNOWN:
                return std::format_to(ctx.out(), "SEC_E_CERT_UNKNOWN");

            case sec_error::_SEC_E_CERT_EXPIRED:
                return std::format_to(ctx.out(), "SEC_E_CERT_EXPIRED");

            case sec_error::_SEC_E_ENCRYPT_FAILURE:
                return std::format_to(ctx.out(), "SEC_E_ENCRYPT_FAILURE");

            case sec_error::_SEC_E_DECRYPT_FAILURE:
                return std::format_to(ctx.out(), "SEC_E_DECRYPT_FAILURE");

            case sec_error::_SEC_E_ALGORITHM_MISMATCH:
                return std::format_to(ctx.out(), "SEC_E_ALGORITHM_MISMATCH");

            case sec_error::_SEC_E_SECURITY_QOS_FAILED:
                return std::format_to(ctx.out(), "SEC_E_SECURITY_QOS_FAILED");

            case sec_error::_SEC_E_UNFINISHED_CONTEXT_DELETED:
                return std::format_to(ctx.out(), "SEC_E_UNFINISHED_CONTEXT_DELETED");

            case sec_error::_SEC_E_NO_TGT_REPLY:
                return std::format_to(ctx.out(), "SEC_E_NO_TGT_REPLY");

            case sec_error::_SEC_E_NO_IP_ADDRESSES:
                return std::format_to(ctx.out(), "SEC_E_NO_IP_ADDRESSES");

            case sec_error::_SEC_E_WRONG_CREDENTIAL_HANDLE:
                return std::format_to(ctx.out(), "SEC_E_WRONG_CREDENTIAL_HANDLE");

            case sec_error::_SEC_E_CRYPTO_SYSTEM_INVALID:
                return std::format_to(ctx.out(), "SEC_E_CRYPTO_SYSTEM_INVALID");

            case sec_error::_SEC_E_MAX_REFERRALS_EXCEEDED:
                return std::format_to(ctx.out(), "SEC_E_MAX_REFERRALS_EXCEEDED");

            case sec_error::_SEC_E_MUST_BE_KDC:
                return std::format_to(ctx.out(), "SEC_E_MUST_BE_KDC");

            case sec_error::_SEC_E_STRONG_CRYPTO_NOT_SUPPORTED:
                return std::format_to(ctx.out(), "SEC_E_STRONG_CRYPTO_NOT_SUPPORTED");

            case sec_error::_SEC_E_TOO_MANY_PRINCIPALS:
                return std::format_to(ctx.out(), "SEC_E_TOO_MANY_PRINCIPALS");

            case sec_error::_SEC_E_NO_PA_DATA:
                return std::format_to(ctx.out(), "SEC_E_NO_PA_DATA");

            case sec_error::_SEC_E_PKINIT_NAME_MISMATCH:
                return std::format_to(ctx.out(), "SEC_E_PKINIT_NAME_MISMATCH");

            case sec_error::_SEC_E_SMARTCARD_LOGON_REQUIRED:
                return std::format_to(ctx.out(), "SEC_E_SMARTCARD_LOGON_REQUIRED");

            case sec_error::_SEC_E_SHUTDOWN_IN_PROGRESS:
                return std::format_to(ctx.out(), "SEC_E_SHUTDOWN_IN_PROGRESS");

            case sec_error::_SEC_E_KDC_INVALID_REQUEST:
                return std::format_to(ctx.out(), "SEC_E_KDC_INVALID_REQUEST");

            case sec_error::_SEC_E_KDC_UNABLE_TO_REFER:
                return std::format_to(ctx.out(), "SEC_E_KDC_UNABLE_TO_REFER");

            case sec_error::_SEC_E_KDC_UNKNOWN_ETYPE:
                return std::format_to(ctx.out(), "SEC_E_KDC_UNKNOWN_ETYPE");

            case sec_error::_SEC_E_UNSUPPORTED_PREAUTH:
                return std::format_to(ctx.out(), "SEC_E_UNSUPPORTED_PREAUTH");

            case sec_error::_SEC_E_DELEGATION_REQUIRED:
                return std::format_to(ctx.out(), "SEC_E_DELEGATION_REQUIRED");

            case sec_error::_SEC_E_BAD_BINDINGS:
                return std::format_to(ctx.out(), "SEC_E_BAD_BINDINGS");

            case sec_error::_SEC_E_MULTIPLE_ACCOUNTS:
                return std::format_to(ctx.out(), "SEC_E_MULTIPLE_ACCOUNTS");

            case sec_error::_SEC_E_NO_KERB_KEY:
                return std::format_to(ctx.out(), "SEC_E_NO_KERB_KEY");

            case sec_error::_SEC_E_CERT_WRONG_USAGE:
                return std::format_to(ctx.out(), "SEC_E_CERT_WRONG_USAGE");

            case sec_error::_SEC_E_DOWNGRADE_DETECTED:
                return std::format_to(ctx.out(), "SEC_E_DOWNGRADE_DETECTED");

            case sec_error::_SEC_E_SMARTCARD_CERT_REVOKED:
                return std::format_to(ctx.out(), "SEC_E_SMARTCARD_CERT_REVOKED");

            case sec_error::_SEC_E_ISSUING_CA_UNTRUSTED:
                return std::format_to(ctx.out(), "SEC_E_ISSUING_CA_UNTRUSTED");

            case sec_error::_SEC_E_REVOCATION_OFFLINE_C:
                return std::format_to(ctx.out(), "SEC_E_REVOCATION_OFFLINE_C");

            case sec_error::_SEC_E_PKINIT_CLIENT_FAILURE:
                return std::format_to(ctx.out(), "SEC_E_PKINIT_CLIENT_FAILURE");

            case sec_error::_SEC_E_SMARTCARD_CERT_EXPIRED:
                return std::format_to(ctx.out(), "SEC_E_SMARTCARD_CERT_EXPIRED");

            case sec_error::_SEC_E_NO_S4U_PROT_SUPPORT:
                return std::format_to(ctx.out(), "SEC_E_NO_S4U_PROT_SUPPORT");

            case sec_error::_SEC_E_CROSSREALM_DELEGATION_FAILURE:
                return std::format_to(ctx.out(), "SEC_E_CROSSREALM_DELEGATION_FAILURE");

            case sec_error::_SEC_E_REVOCATION_OFFLINE_KDC:
                return std::format_to(ctx.out(), "SEC_E_REVOCATION_OFFLINE_KDC");

            case sec_error::_SEC_E_ISSUING_CA_UNTRUSTED_KDC:
                return std::format_to(ctx.out(), "SEC_E_ISSUING_CA_UNTRUSTED_KDC");

            case sec_error::_SEC_E_KDC_CERT_EXPIRED:
                return std::format_to(ctx.out(), "SEC_E_KDC_CERT_EXPIRED");

            case sec_error::_SEC_E_KDC_CERT_REVOKED:
                return std::format_to(ctx.out(), "SEC_E_KDC_CERT_REVOKED");

            case sec_error::_SEC_I_SIGNATURE_NEEDED:
                return std::format_to(ctx.out(), "SEC_I_SIGNATURE_NEEDED");

            case sec_error::_SEC_E_INVALID_PARAMETER:
                return std::format_to(ctx.out(), "SEC_E_INVALID_PARAMETER");

            case sec_error::_SEC_E_DELEGATION_POLICY:
                return std::format_to(ctx.out(), "SEC_E_DELEGATION_POLICY");

            case sec_error::_SEC_E_POLICY_NLTM_ONLY:
                return std::format_to(ctx.out(), "SEC_E_POLICY_NLTM_ONLY");

            case sec_error::_SEC_I_NO_RENEGOTIATION:
                return std::format_to(ctx.out(), "SEC_I_NO_RENEGOTIATION");

            case sec_error::_SEC_E_NO_CONTEXT:
                return std::format_to(ctx.out(), "SEC_E_NO_CONTEXT");

            case sec_error::_SEC_E_PKU2U_CERT_FAILURE:
                return std::format_to(ctx.out(), "SEC_E_PKU2U_CERT_FAILURE");

            case sec_error::_SEC_E_MUTUAL_AUTH_FAILED:
                return std::format_to(ctx.out(), "SEC_E_MUTUAL_AUTH_FAILED");

            case sec_error::_SEC_I_MESSAGE_FRAGMENT:
                return std::format_to(ctx.out(), "SEC_I_MESSAGE_FRAGMENT");

            case sec_error::_SEC_E_ONLY_HTTPS_ALLOWED:
                return std::format_to(ctx.out(), "SEC_E_ONLY_HTTPS_ALLOWED");

            case sec_error::_SEC_I_CONTINUE_NEEDED_MESSAGE_OK:
                return std::format_to(ctx.out(), "SEC_I_CONTINUE_NEEDED_MESSAGE_OK");

            case sec_error::_SEC_E_APPLICATION_PROTOCOL_MISMATCH:
                return std::format_to(ctx.out(), "SEC_E_APPLICATION_PROTOCOL_MISMATCH");

            case sec_error::_SEC_I_ASYNC_CALL_PENDING:
                return std::format_to(ctx.out(), "SEC_I_ASYNC_CALL_PENDING");

            case sec_error::_SEC_E_INVALID_UPN_NAME:
                return std::format_to(ctx.out(), "SEC_E_INVALID_UPN_NAME");

            case sec_error::_SEC_E_EXT_BUFFER_TOO_SMALL:
                return std::format_to(ctx.out(), "SEC_E_EXT_BUFFER_TOO_SMALL");

            case sec_error::_SEC_E_INSUFFICIENT_BUFFERS:
                return std::format_to(ctx.out(), "SEC_E_INSUFFICIENT_BUFFERS");

            default:
                return std::format_to(ctx.out(), "{:08x}", (uint32_t)t);
        }
    }
};
#endif

#ifdef HAVE_GSSAPI
enum class krb5_minor {
    KRB5KDC_ERR_NONE = -1765328384L,
    KRB5KDC_ERR_NAME_EXP = -1765328383L,
    KRB5KDC_ERR_SERVICE_EXP = -1765328382L,
    KRB5KDC_ERR_BAD_PVNO = -1765328381L,
    KRB5KDC_ERR_C_OLD_MAST_KVNO = -1765328380L,
    KRB5KDC_ERR_S_OLD_MAST_KVNO = -1765328379L,
    KRB5KDC_ERR_C_PRINCIPAL_UNKNOWN = -1765328378L,
    KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN = -1765328377L,
    KRB5KDC_ERR_PRINCIPAL_NOT_UNIQUE = -1765328376L,
    KRB5KDC_ERR_NULL_KEY = -1765328375L,
    KRB5KDC_ERR_CANNOT_POSTDATE = -1765328374L,
    KRB5KDC_ERR_NEVER_VALID = -1765328373L,
    KRB5KDC_ERR_POLICY = -1765328372L,
    KRB5KDC_ERR_BADOPTION = -1765328371L,
    KRB5KDC_ERR_ETYPE_NOSUPP = -1765328370L,
    KRB5KDC_ERR_SUMTYPE_NOSUPP = -1765328369L,
    KRB5KDC_ERR_PADATA_TYPE_NOSUPP = -1765328368L,
    KRB5KDC_ERR_TRTYPE_NOSUPP = -1765328367L,
    KRB5KDC_ERR_CLIENT_REVOKED = -1765328366L,
    KRB5KDC_ERR_SERVICE_REVOKED = -1765328365L,
    KRB5KDC_ERR_TGT_REVOKED = -1765328364L,
    KRB5KDC_ERR_CLIENT_NOTYET = -1765328363L,
    KRB5KDC_ERR_SERVICE_NOTYET = -1765328362L,
    KRB5KDC_ERR_KEY_EXP = -1765328361L,
    KRB5KDC_ERR_PREAUTH_FAILED = -1765328360L,
    KRB5KDC_ERR_PREAUTH_REQUIRED = -1765328359L,
    KRB5KDC_ERR_SERVER_NOMATCH = -1765328358L,
    KRB5KDC_ERR_MUST_USE_USER2USER = -1765328357L,
    KRB5KDC_ERR_PATH_NOT_ACCEPTED = -1765328356L,
    KRB5KDC_ERR_SVC_UNAVAILABLE = -1765328355L,
    KRB5PLACEHOLD_30 = -1765328354L,
    KRB5KRB_AP_ERR_BAD_INTEGRITY = -1765328353L,
    KRB5KRB_AP_ERR_TKT_EXPIRED = -1765328352L,
    KRB5KRB_AP_ERR_TKT_NYV = -1765328351L,
    KRB5KRB_AP_ERR_REPEAT = -1765328350L,
    KRB5KRB_AP_ERR_NOT_US = -1765328349L,
    KRB5KRB_AP_ERR_BADMATCH = -1765328348L,
    KRB5KRB_AP_ERR_SKEW = -1765328347L,
    KRB5KRB_AP_ERR_BADADDR = -1765328346L,
    KRB5KRB_AP_ERR_BADVERSION = -1765328345L,
    KRB5KRB_AP_ERR_MSG_TYPE = -1765328344L,
    KRB5KRB_AP_ERR_MODIFIED = -1765328343L,
    KRB5KRB_AP_ERR_BADORDER = -1765328342L,
    KRB5KRB_AP_ERR_ILL_CR_TKT = -1765328341L,
    KRB5KRB_AP_ERR_BADKEYVER = -1765328340L,
    KRB5KRB_AP_ERR_NOKEY = -1765328339L,
    KRB5KRB_AP_ERR_MUT_FAIL = -1765328338L,
    KRB5KRB_AP_ERR_BADDIRECTION = -1765328337L,
    KRB5KRB_AP_ERR_METHOD = -1765328336L,
    KRB5KRB_AP_ERR_BADSEQ = -1765328335L,
    KRB5KRB_AP_ERR_INAPP_CKSUM = -1765328334L,
    KRB5KRB_AP_PATH_NOT_ACCEPTED = -1765328333L,
    KRB5KRB_ERR_RESPONSE_TOO_BIG = -1765328332L,
    KRB5PLACEHOLD_53 = -1765328331L,
    KRB5PLACEHOLD_54 = -1765328330L,
    KRB5PLACEHOLD_55 = -1765328329L,
    KRB5PLACEHOLD_56 = -1765328328L,
    KRB5PLACEHOLD_57 = -1765328327L,
    KRB5PLACEHOLD_58 = -1765328326L,
    KRB5PLACEHOLD_59 = -1765328325L,
    KRB5KRB_ERR_GENERIC = -1765328324L,
    KRB5KRB_ERR_FIELD_TOOLONG = -1765328323L,
    KRB5KDC_ERR_CLIENT_NOT_TRUSTED = -1765328322L,
    KRB5KDC_ERR_KDC_NOT_TRUSTED = -1765328321L,
    KRB5KDC_ERR_INVALID_SIG = -1765328320L,
    KRB5KDC_ERR_DH_KEY_PARAMETERS_NOT_ACCEPTED = -1765328319L,
    KRB5KDC_ERR_CERTIFICATE_MISMATCH = -1765328318L,
    KRB5KRB_AP_ERR_NO_TGT = -1765328317L,
    KRB5KDC_ERR_WRONG_REALM = -1765328316L,
    KRB5KRB_AP_ERR_USER_TO_USER_REQUIRED = -1765328315L,
    KRB5KDC_ERR_CANT_VERIFY_CERTIFICATE = -1765328314L,
    KRB5KDC_ERR_INVALID_CERTIFICATE = -1765328313L,
    KRB5KDC_ERR_REVOKED_CERTIFICATE = -1765328312L,
    KRB5KDC_ERR_REVOCATION_STATUS_UNKNOWN = -1765328311L,
    KRB5KDC_ERR_REVOCATION_STATUS_UNAVAILABLE = -1765328310L,
    KRB5KDC_ERR_CLIENT_NAME_MISMATCH = -1765328309L,
    KRB5KDC_ERR_KDC_NAME_MISMATCH = -1765328308L,
    KRB5KDC_ERR_INCONSISTENT_KEY_PURPOSE = -1765328307L,
    KRB5KDC_ERR_DIGEST_IN_CERT_NOT_ACCEPTED = -1765328306L,
    KRB5KDC_ERR_PA_CHECKSUM_MUST_BE_INCLUDED = -1765328305L,
    KRB5KDC_ERR_DIGEST_IN_SIGNED_DATA_NOT_ACCEPTED = -1765328304L,
    KRB5KDC_ERR_PUBLIC_KEY_ENCRYPTION_NOT_SUPPORTED = -1765328303L,
    KRB5PLACEHOLD_82 = -1765328302L,
    KRB5PLACEHOLD_83 = -1765328301L,
    KRB5PLACEHOLD_84 = -1765328300L,
    KRB5KRB_AP_ERR_IAKERB_KDC_NOT_FOUND = -1765328299L,
    KRB5KRB_AP_ERR_IAKERB_KDC_NO_RESPONSE = -1765328298L,
    KRB5PLACEHOLD_87 = -1765328297L,
    KRB5PLACEHOLD_88 = -1765328296L,
    KRB5PLACEHOLD_89 = -1765328295L,
    KRB5KDC_ERR_PREAUTH_EXPIRED = -1765328294L,
    KRB5KDC_ERR_MORE_PREAUTH_DATA_REQUIRED = -1765328293L,
    KRB5PLACEHOLD_92 = -1765328292L,
    KRB5KDC_ERR_UNKNOWN_CRITICAL_FAST_OPTION = -1765328291L,
    KRB5PLACEHOLD_94 = -1765328290L,
    KRB5PLACEHOLD_95 = -1765328289L,
    KRB5PLACEHOLD_96 = -1765328288L,
    KRB5PLACEHOLD_97 = -1765328287L,
    KRB5PLACEHOLD_98 = -1765328286L,
    KRB5PLACEHOLD_99 = -1765328285L,
    KRB5KDC_ERR_NO_ACCEPTABLE_KDF = -1765328284L,
    KRB5PLACEHOLD_101 = -1765328283L,
    KRB5PLACEHOLD_102 = -1765328282L,
    KRB5PLACEHOLD_103 = -1765328281L,
    KRB5PLACEHOLD_104 = -1765328280L,
    KRB5PLACEHOLD_105 = -1765328279L,
    KRB5PLACEHOLD_106 = -1765328278L,
    KRB5PLACEHOLD_107 = -1765328277L,
    KRB5PLACEHOLD_108 = -1765328276L,
    KRB5PLACEHOLD_109 = -1765328275L,
    KRB5PLACEHOLD_110 = -1765328274L,
    KRB5PLACEHOLD_111 = -1765328273L,
    KRB5PLACEHOLD_112 = -1765328272L,
    KRB5PLACEHOLD_113 = -1765328271L,
    KRB5PLACEHOLD_114 = -1765328270L,
    KRB5PLACEHOLD_115 = -1765328269L,
    KRB5PLACEHOLD_116 = -1765328268L,
    KRB5PLACEHOLD_117 = -1765328267L,
    KRB5PLACEHOLD_118 = -1765328266L,
    KRB5PLACEHOLD_119 = -1765328265L,
    KRB5PLACEHOLD_120 = -1765328264L,
    KRB5PLACEHOLD_121 = -1765328263L,
    KRB5PLACEHOLD_122 = -1765328262L,
    KRB5PLACEHOLD_123 = -1765328261L,
    KRB5PLACEHOLD_124 = -1765328260L,
    KRB5PLACEHOLD_125 = -1765328259L,
    KRB5PLACEHOLD_126 = -1765328258L,
    KRB5PLACEHOLD_127 = -1765328257L,
    KRB5_ERR_RCSID = -1765328256L,
    KRB5_LIBOS_BADLOCKFLAG = -1765328255L,
    KRB5_LIBOS_CANTREADPWD = -1765328254L,
    KRB5_LIBOS_BADPWDMATCH = -1765328253L,
    KRB5_LIBOS_PWDINTR = -1765328252L,
    KRB5_PARSE_ILLCHAR = -1765328251L,
    KRB5_PARSE_MALFORMED = -1765328250L,
    KRB5_CONFIG_CANTOPEN = -1765328249L,
    KRB5_CONFIG_BADFORMAT = -1765328248L,
    KRB5_CONFIG_NOTENUFSPACE = -1765328247L,
    KRB5_BADMSGTYPE = -1765328246L,
    KRB5_CC_BADNAME = -1765328245L,
    KRB5_CC_UNKNOWN_TYPE = -1765328244L,
    KRB5_CC_NOTFOUND = -1765328243L,
    KRB5_CC_END = -1765328242L,
    KRB5_NO_TKT_SUPPLIED = -1765328241L,
    KRB5KRB_AP_WRONG_PRINC = -1765328240L,
    KRB5KRB_AP_ERR_TKT_INVALID = -1765328239L,
    KRB5_PRINC_NOMATCH = -1765328238L,
    KRB5_KDCREP_MODIFIED = -1765328237L,
    KRB5_KDCREP_SKEW = -1765328236L,
    KRB5_IN_TKT_REALM_MISMATCH = -1765328235L,
    KRB5_PROG_ETYPE_NOSUPP = -1765328234L,
    KRB5_PROG_KEYTYPE_NOSUPP = -1765328233L,
    KRB5_WRONG_ETYPE = -1765328232L,
    KRB5_PROG_SUMTYPE_NOSUPP = -1765328231L,
    KRB5_REALM_UNKNOWN = -1765328230L,
    KRB5_SERVICE_UNKNOWN = -1765328229L,
    KRB5_KDC_UNREACH = -1765328228L,
    KRB5_NO_LOCALNAME = -1765328227L,
    KRB5_MUTUAL_FAILED = -1765328226L,
    KRB5_RC_TYPE_EXISTS = -1765328225L,
    KRB5_RC_MALLOC = -1765328224L,
    KRB5_RC_TYPE_NOTFOUND = -1765328223L,
    KRB5_RC_UNKNOWN = -1765328222L,
    KRB5_RC_REPLAY = -1765328221L,
    KRB5_RC_IO = -1765328220L,
    KRB5_RC_NOIO = -1765328219L,
    KRB5_RC_PARSE = -1765328218L,
    KRB5_RC_IO_EOF = -1765328217L,
    KRB5_RC_IO_MALLOC = -1765328216L,
    KRB5_RC_IO_PERM = -1765328215L,
    KRB5_RC_IO_IO = -1765328214L,
    KRB5_RC_IO_UNKNOWN = -1765328213L,
    KRB5_RC_IO_SPACE = -1765328212L,
    KRB5_TRANS_CANTOPEN = -1765328211L,
    KRB5_TRANS_BADFORMAT = -1765328210L,
    KRB5_LNAME_CANTOPEN = -1765328209L,
    KRB5_LNAME_NOTRANS = -1765328208L,
    KRB5_LNAME_BADFORMAT = -1765328207L,
    KRB5_CRYPTO_INTERNAL = -1765328206L,
    KRB5_KT_BADNAME = -1765328205L,
    KRB5_KT_UNKNOWN_TYPE = -1765328204L,
    KRB5_KT_NOTFOUND = -1765328203L,
    KRB5_KT_END = -1765328202L,
    KRB5_KT_NOWRITE = -1765328201L,
    KRB5_KT_IOERR = -1765328200L,
    KRB5_NO_TKT_IN_RLM = -1765328199L,
    KRB5DES_BAD_KEYPAR = -1765328198L,
    KRB5DES_WEAK_KEY = -1765328197L,
    KRB5_BAD_ENCTYPE = -1765328196L,
    KRB5_BAD_KEYSIZE = -1765328195L,
    KRB5_BAD_MSIZE = -1765328194L,
    KRB5_CC_TYPE_EXISTS = -1765328193L,
    KRB5_KT_TYPE_EXISTS = -1765328192L,
    KRB5_CC_IO = -1765328191L,
    KRB5_FCC_PERM = -1765328190L,
    KRB5_FCC_NOFILE = -1765328189L,
    KRB5_FCC_INTERNAL = -1765328188L,
    KRB5_CC_WRITE = -1765328187L,
    KRB5_CC_NOMEM = -1765328186L,
    KRB5_CC_FORMAT = -1765328185L,
    KRB5_CC_NOT_KTYPE = -1765328184L,
    KRB5_INVALID_FLAGS = -1765328183L,
    KRB5_NO_2ND_TKT = -1765328182L,
    KRB5_NOCREDS_SUPPLIED = -1765328181L,
    KRB5_SENDAUTH_BADAUTHVERS = -1765328180L,
    KRB5_SENDAUTH_BADAPPLVERS = -1765328179L,
    KRB5_SENDAUTH_BADRESPONSE = -1765328178L,
    KRB5_SENDAUTH_REJECTED = -1765328177L,
    KRB5_PREAUTH_BAD_TYPE = -1765328176L,
    KRB5_PREAUTH_NO_KEY = -1765328175L,
    KRB5_PREAUTH_FAILED = -1765328174L,
    KRB5_RCACHE_BADVNO = -1765328173L,
    KRB5_CCACHE_BADVNO = -1765328172L,
    KRB5_KEYTAB_BADVNO = -1765328171L,
    KRB5_PROG_ATYPE_NOSUPP = -1765328170L,
    KRB5_RC_REQUIRED = -1765328169L,
    KRB5_ERR_BAD_HOSTNAME = -1765328168L,
    KRB5_ERR_HOST_REALM_UNKNOWN = -1765328167L,
    KRB5_SNAME_UNSUPP_NAMETYPE = -1765328166L,
    KRB5KRB_AP_ERR_V4_REPLY = -1765328165L,
    KRB5_REALM_CANT_RESOLVE = -1765328164L,
    KRB5_TKT_NOT_FORWARDABLE = -1765328163L,
    KRB5_FWD_BAD_PRINCIPAL = -1765328162L,
    KRB5_GET_IN_TKT_LOOP = -1765328161L,
    KRB5_CONFIG_NODEFREALM = -1765328160L,
    KRB5_SAM_UNSUPPORTED = -1765328159L,
    KRB5_SAM_INVALID_ETYPE = -1765328158L,
    KRB5_SAM_NO_CHECKSUM = -1765328157L,
    KRB5_SAM_BAD_CHECKSUM = -1765328156L,
    KRB5_KT_NAME_TOOLONG = -1765328155L,
    KRB5_KT_KVNONOTFOUND = -1765328154L,
    KRB5_APPL_EXPIRED = -1765328153L,
    KRB5_LIB_EXPIRED = -1765328152L,
    KRB5_CHPW_PWDNULL = -1765328151L,
    KRB5_CHPW_FAIL = -1765328150L,
    KRB5_KT_FORMAT = -1765328149L,
    KRB5_NOPERM_ETYPE = -1765328148L,
    KRB5_CONFIG_ETYPE_NOSUPP = -1765328147L,
    KRB5_OBSOLETE_FN = -1765328146L,
    KRB5_EAI_FAIL = -1765328145L,
    KRB5_EAI_NODATA = -1765328144L,
    KRB5_EAI_NONAME = -1765328143L,
    KRB5_EAI_SERVICE = -1765328142L,
    KRB5_ERR_NUMERIC_REALM = -1765328141L,
    KRB5_ERR_BAD_S2K_PARAMS = -1765328140L,
    KRB5_ERR_NO_SERVICE = -1765328139L,
    KRB5_CC_READONLY = -1765328138L,
    KRB5_CC_NOSUPP = -1765328137L,
    KRB5_DELTAT_BADFORMAT = -1765328136L,
    KRB5_PLUGIN_NO_HANDLE = -1765328135L,
    KRB5_PLUGIN_OP_NOTSUPP = -1765328134L,
    KRB5_ERR_INVALID_UTF8 = -1765328133L,
    KRB5_ERR_FAST_REQUIRED = -1765328132L,
    KRB5_LOCAL_ADDR_REQUIRED = -1765328131L,
    KRB5_REMOTE_ADDR_REQUIRED = -1765328130L,
    KRB5_TRACE_NOSUPP = -1765328129L
};
#endif

static constexpr int ymd_to_num(const std::chrono::year_month_day& d) noexcept {
    int m2 = ((int)(unsigned int)d.month() - 14) / 12;
    long long n;

    n = (1461 * ((int)d.year() + 4800 + m2)) / 4;
    n += (367 * ((int)(unsigned int)d.month() - 2 - (12 * m2))) / 12;
    n -= (3 * (((int)d.year() + 4900 + m2)/100)) / 4;
    n += (unsigned int)d.day();
    n -= 2447096;

    return static_cast<int>(n);
}

static_assert(ymd_to_num({std::chrono::year{1}, std::chrono::January, std::chrono::day{1}}) == -693595);
static_assert(ymd_to_num({std::chrono::year{1900}, std::chrono::January, std::chrono::day{1}}) == 0);

static constexpr std::chrono::year_month_day num_to_ymd(int num) noexcept {
    signed long long j, e, f, g, h;
    uint8_t day, month;
    uint16_t year;

    j = num + 2415021;

    f = (4 * j) + 274277;
    f /= 146097;
    f *= 3;
    f /= 4;
    f += j;
    f += 1363;

    e = (4 * f) + 3;
    g = (e % 1461) / 4;
    h = (5 * g) + 2;

    day = (uint8_t)(((h % 153) / 5) + 1);
    month = (uint8_t)(((h / 153) + 2) % 12 + 1);
    year = static_cast<uint16_t>((e / 1461) - 4716 + ((14 - month) / 12));

    return {std::chrono::year{year}, std::chrono::month{month}, std::chrono::day{day}};
}

static_assert(num_to_ymd(-693595) == std::chrono::year_month_day{std::chrono::year{1}, std::chrono::January, std::chrono::day{1}});
static_assert(num_to_ymd(0) == std::chrono::year_month_day{std::chrono::year{1900}, std::chrono::January, std::chrono::day{1}});

static __inline std::u16string_view extract_message(std::span<const uint8_t> sp) {
    return std::u16string_view((char16_t*)&sp[8], *(uint16_t*)&sp[6]);
}

template<unsigned N>
static void buf_lshift(uint8_t* scratch) {
    bool carry = false;

    for (unsigned int i = 0; i < N; i++) {
        bool b = scratch[i] & 0x80;

        scratch[i] <<= 1;

        if (carry)
            scratch[i] |= 1;

        carry = b;
    }
}

template<unsigned N>
static void buf_rshift(uint8_t* scratch) {
    bool carry = false;

    for (int i = N - 1; i >= 0; i--) {
        bool b = scratch[i] & 0x1;

        scratch[i] >>= 1;

        if (carry)
            scratch[i] |= 0x80;

        carry = b;
    }
}

static constexpr bool is_byte_len_type(enum tds::sql_type type) noexcept {
    switch (type) {
        case tds::sql_type::UNIQUEIDENTIFIER:
        case tds::sql_type::INTN:
        case tds::sql_type::DECIMAL:
        case tds::sql_type::NUMERIC:
        case tds::sql_type::BITN:
        case tds::sql_type::FLTN:
        case tds::sql_type::MONEYN:
        case tds::sql_type::DATETIMN:
        case tds::sql_type::DATE:
        case tds::sql_type::TIME:
        case tds::sql_type::DATETIME2:
        case tds::sql_type::DATETIMEOFFSET:
            return true;

        default:
            return false;
    }
}

// tdscpp.cpp
std::span<const uint8_t> parse_tokens(std::span<const uint8_t> sp, std::list<std::vector<uint8_t>>& tokens,
                                      std::vector<tds::column>& buf_columns, uint64_t& varchar_left);
void handle_row_col(tds::value_data_t& val, bool& is_null, enum tds::sql_type type,
                    unsigned int max_length, std::span<const uint8_t>& sp);
void handle_nbcrow(std::span<const uint8_t>& sp, const std::vector<tds::column>& cols,
                   std::list<std::vector<std::pair<tds::value_data_t, bool>>>& rows);

// ver80coll.cpp
std::weak_ordering compare_strings_80(std::u16string_view val1, std::u16string_view val2,
                                      const tds::collation& coll);

// ver90coll.cpp
std::weak_ordering compare_strings_90(std::u16string_view val1, std::u16string_view val2,
                                      const tds::collation& coll);

// ver100coll.cpp
std::weak_ordering compare_strings_100(std::u16string_view val1, std::u16string_view val2,
                                       const tds::collation& coll);

// ver140coll.cpp
std::weak_ordering compare_strings_140(std::u16string_view val1, std::u16string_view val2,
                                       const tds::collation& coll);
