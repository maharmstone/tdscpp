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
#include "tdscpp-private.h"
#include "ringbuf.h"
#if !defined(WITH_OPENSSL) && defined(_WIN32)
#include <schannel.h>
#endif

using namespace std;

#ifdef WITH_OPENSSL
class ssl_error : public exception {
public:
    ssl_error(const char* func, unsigned long err) {
        auto str = ERR_reason_error_string(err);

        if (str)
            msg = str;
        else
            msg = func + " failed: "s + to_string(err);
    }

    const char* what() const noexcept {
        return msg.c_str();
    }

private:
    string msg;
};

static int ssl_bio_read(BIO* bio, char* data, int len) noexcept {
    auto& t = *(tds::tds_ssl*)BIO_get_data(bio);

    try {
        return t.ssl_read_cb(span{(uint8_t*)data, (size_t)len});
    } catch (...) {
        t.exception = current_exception();
        return -1;
    }
}

static int ssl_bio_write(BIO* bio, const char* data, int len) noexcept {
    auto& t = *(tds::tds_ssl*)BIO_get_data(bio);

    try {
        return t.ssl_write_cb(span{(uint8_t*)data, (size_t)len});
    } catch (...) {
        t.exception = current_exception();
        return -1;
    }
}

static long ssl_bio_ctrl(BIO* bio, int cmd, long num, void* ptr) noexcept {
    auto& t = *(tds::tds_ssl*)BIO_get_data(bio);

    try {
        return t.ssl_ctrl_cb(cmd, num, ptr);
    } catch (...) {
        t.exception = current_exception();
        return -1;
    }
}

static string x509_err_to_string(int err) {
    switch (err) {
        case X509_V_OK:
            return "X509_V_OK";
        case X509_V_ERR_UNSPECIFIED:
            return "X509_V_ERR_UNSPECIFIED";
        case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT:
            return "X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT";
        case X509_V_ERR_UNABLE_TO_GET_CRL:
            return "X509_V_ERR_UNABLE_TO_GET_CRL";
        case X509_V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE:
            return "X509_V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE";
        case X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE:
            return "X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE";
        case X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY:
            return "X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY";
        case X509_V_ERR_CERT_SIGNATURE_FAILURE:
            return "X509_V_ERR_CERT_SIGNATURE_FAILURE";
        case X509_V_ERR_CRL_SIGNATURE_FAILURE:
            return "X509_V_ERR_CRL_SIGNATURE_FAILURE";
        case X509_V_ERR_CERT_NOT_YET_VALID:
            return "X509_V_ERR_CERT_NOT_YET_VALID";
        case X509_V_ERR_CERT_HAS_EXPIRED:
            return "X509_V_ERR_CERT_HAS_EXPIRED";
        case X509_V_ERR_CRL_NOT_YET_VALID:
            return "X509_V_ERR_CRL_NOT_YET_VALID";
        case X509_V_ERR_CRL_HAS_EXPIRED:
            return "X509_V_ERR_CRL_HAS_EXPIRED";
        case X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD:
            return "X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD";
        case X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD:
            return "X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD";
        case X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD:
            return "X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD";
        case X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD:
            return "X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD";
        case X509_V_ERR_OUT_OF_MEM:
            return "X509_V_ERR_OUT_OF_MEM";
        case X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT:
            return "X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT";
        case X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN:
            return "X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN";
        case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY:
            return "X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY";
        case X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE:
            return "X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE";
        case X509_V_ERR_CERT_CHAIN_TOO_LONG:
            return "X509_V_ERR_CERT_CHAIN_TOO_LONG";
        case X509_V_ERR_CERT_REVOKED:
            return "X509_V_ERR_CERT_REVOKED";
        case X509_V_ERR_INVALID_CA:
            return "X509_V_ERR_INVALID_CA";
        case X509_V_ERR_PATH_LENGTH_EXCEEDED:
            return "X509_V_ERR_PATH_LENGTH_EXCEEDED";
        case X509_V_ERR_INVALID_PURPOSE:
            return "X509_V_ERR_INVALID_PURPOSE";
        case X509_V_ERR_CERT_UNTRUSTED:
            return "X509_V_ERR_CERT_UNTRUSTED";
        case X509_V_ERR_CERT_REJECTED:
            return "X509_V_ERR_CERT_REJECTED";
        case X509_V_ERR_SUBJECT_ISSUER_MISMATCH:
            return "X509_V_ERR_SUBJECT_ISSUER_MISMATCH";
        case X509_V_ERR_AKID_SKID_MISMATCH:
            return "X509_V_ERR_AKID_SKID_MISMATCH";
        case X509_V_ERR_AKID_ISSUER_SERIAL_MISMATCH:
            return "X509_V_ERR_AKID_ISSUER_SERIAL_MISMATCH";
        case X509_V_ERR_KEYUSAGE_NO_CERTSIGN:
            return "X509_V_ERR_KEYUSAGE_NO_CERTSIGN";
        case X509_V_ERR_UNABLE_TO_GET_CRL_ISSUER:
            return "X509_V_ERR_UNABLE_TO_GET_CRL_ISSUER";
        case X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION:
            return "X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION";
        case X509_V_ERR_KEYUSAGE_NO_CRL_SIGN:
            return "X509_V_ERR_KEYUSAGE_NO_CRL_SIGN";
        case X509_V_ERR_UNHANDLED_CRITICAL_CRL_EXTENSION:
            return "X509_V_ERR_UNHANDLED_CRITICAL_CRL_EXTENSION";
        case X509_V_ERR_INVALID_NON_CA:
            return "X509_V_ERR_INVALID_NON_CA";
        case X509_V_ERR_PROXY_PATH_LENGTH_EXCEEDED:
            return "X509_V_ERR_PROXY_PATH_LENGTH_EXCEEDED";
        case X509_V_ERR_KEYUSAGE_NO_DIGITAL_SIGNATURE:
            return "X509_V_ERR_KEYUSAGE_NO_DIGITAL_SIGNATURE";
        case X509_V_ERR_PROXY_CERTIFICATES_NOT_ALLOWED:
            return "X509_V_ERR_PROXY_CERTIFICATES_NOT_ALLOWED";
        case X509_V_ERR_INVALID_EXTENSION:
            return "X509_V_ERR_INVALID_EXTENSION";
        case X509_V_ERR_INVALID_POLICY_EXTENSION:
            return "X509_V_ERR_INVALID_POLICY_EXTENSION";
        case X509_V_ERR_NO_EXPLICIT_POLICY:
            return "X509_V_ERR_NO_EXPLICIT_POLICY";
        case X509_V_ERR_DIFFERENT_CRL_SCOPE:
            return "X509_V_ERR_DIFFERENT_CRL_SCOPE";
        case X509_V_ERR_UNSUPPORTED_EXTENSION_FEATURE:
            return "X509_V_ERR_UNSUPPORTED_EXTENSION_FEATURE";
        case X509_V_ERR_UNNESTED_RESOURCE:
            return "X509_V_ERR_UNNESTED_RESOURCE";
        case X509_V_ERR_PERMITTED_VIOLATION:
            return "X509_V_ERR_PERMITTED_VIOLATION";
        case X509_V_ERR_EXCLUDED_VIOLATION:
            return "X509_V_ERR_EXCLUDED_VIOLATION";
        case X509_V_ERR_SUBTREE_MINMAX:
            return "X509_V_ERR_SUBTREE_MINMAX";
        case X509_V_ERR_APPLICATION_VERIFICATION:
            return "X509_V_ERR_APPLICATION_VERIFICATION";
        case X509_V_ERR_UNSUPPORTED_CONSTRAINT_TYPE:
            return "X509_V_ERR_UNSUPPORTED_CONSTRAINT_TYPE";
        case X509_V_ERR_UNSUPPORTED_CONSTRAINT_SYNTAX:
            return "X509_V_ERR_UNSUPPORTED_CONSTRAINT_SYNTAX";
        case X509_V_ERR_UNSUPPORTED_NAME_SYNTAX:
            return "X509_V_ERR_UNSUPPORTED_NAME_SYNTAX";
        case X509_V_ERR_CRL_PATH_VALIDATION_ERROR:
            return "X509_V_ERR_CRL_PATH_VALIDATION_ERROR";
        case X509_V_ERR_PATH_LOOP:
            return "X509_V_ERR_PATH_LOOP";
        case X509_V_ERR_SUITE_B_INVALID_VERSION:
            return "X509_V_ERR_SUITE_B_INVALID_VERSION";
        case X509_V_ERR_SUITE_B_INVALID_ALGORITHM:
            return "X509_V_ERR_SUITE_B_INVALID_ALGORITHM";
        case X509_V_ERR_SUITE_B_INVALID_CURVE:
            return "X509_V_ERR_SUITE_B_INVALID_CURVE";
        case X509_V_ERR_SUITE_B_INVALID_SIGNATURE_ALGORITHM:
            return "X509_V_ERR_SUITE_B_INVALID_SIGNATURE_ALGORITHM";
        case X509_V_ERR_SUITE_B_LOS_NOT_ALLOWED:
            return "X509_V_ERR_SUITE_B_LOS_NOT_ALLOWED";
        case X509_V_ERR_SUITE_B_CANNOT_SIGN_P_384_WITH_P_256:
            return "X509_V_ERR_SUITE_B_CANNOT_SIGN_P_384_WITH_P_256";
        case X509_V_ERR_HOSTNAME_MISMATCH:
            return "X509_V_ERR_HOSTNAME_MISMATCH";
        case X509_V_ERR_EMAIL_MISMATCH:
            return "X509_V_ERR_EMAIL_MISMATCH";
        case X509_V_ERR_IP_ADDRESS_MISMATCH:
            return "X509_V_ERR_IP_ADDRESS_MISMATCH";
        case X509_V_ERR_DANE_NO_MATCH:
            return "X509_V_ERR_DANE_NO_MATCH";
        case X509_V_ERR_EE_KEY_TOO_SMALL:
            return "X509_V_ERR_EE_KEY_TOO_SMALL";
        case X509_V_ERR_CA_KEY_TOO_SMALL:
            return "X509_V_ERR_CA_KEY_TOO_SMALL";
        case X509_V_ERR_CA_MD_TOO_WEAK:
            return "X509_V_ERR_CA_MD_TOO_WEAK";
        case X509_V_ERR_INVALID_CALL:
            return "X509_V_ERR_INVALID_CALL";
        case X509_V_ERR_STORE_LOOKUP:
            return "X509_V_ERR_STORE_LOOKUP";
        case X509_V_ERR_NO_VALID_SCTS:
            return "X509_V_ERR_NO_VALID_SCTS";
        case X509_V_ERR_PROXY_SUBJECT_NAME_VIOLATION:
            return "X509_V_ERR_PROXY_SUBJECT_NAME_VIOLATION";
        case X509_V_ERR_OCSP_VERIFY_NEEDED:
            return "X509_V_ERR_OCSP_VERIFY_NEEDED";
        case X509_V_ERR_OCSP_VERIFY_FAILED:
            return "X509_V_ERR_OCSP_VERIFY_FAILED";
        case X509_V_ERR_OCSP_CERT_UNKNOWN:
            return "X509_V_ERR_OCSP_CERT_UNKNOWN";
        case X509_V_ERR_SIGNATURE_ALGORITHM_MISMATCH:
            return "X509_V_ERR_SIGNATURE_ALGORITHM_MISMATCH";
        case X509_V_ERR_NO_ISSUER_PUBLIC_KEY:
            return "X509_V_ERR_NO_ISSUER_PUBLIC_KEY";
        case X509_V_ERR_UNSUPPORTED_SIGNATURE_ALGORITHM:
            return "X509_V_ERR_UNSUPPORTED_SIGNATURE_ALGORITHM";
        case X509_V_ERR_EC_KEY_EXPLICIT_PARAMS:
            return "X509_V_ERR_EC_KEY_EXPLICIT_PARAMS";
        default:
            return to_string(err);
    }
}

static int verify_callback(int preverify, X509_STORE_CTX* x509_ctx) noexcept {
    auto ssl = (SSL*)X509_STORE_CTX_get_ex_data(x509_ctx, SSL_get_ex_data_X509_STORE_CTX_idx());
    auto& c = *(tds::tds_ssl*)SSL_get_ex_data(ssl, 0);

    try {
        return c.ssl_verify_cb(preverify, x509_ctx);
    } catch (...) {
        c.exception = current_exception();
        return 0;
    }
}

#ifdef _WIN32
class cert_store_closer {
public:
    typedef HCERTSTORE pointer;

    void operator()(HCERTSTORE h) {
        CertCloseStore(h, 0);
    }
};

class x509_closer {
public:
    typedef X509* pointer;

    void operator()(X509* x) {
        X509_free(x);
    }
};

static void add_certs_to_store(X509_STORE* store) {
    PCCERT_CONTEXT certctx = nullptr;

    unique_ptr<HCERTSTORE, cert_store_closer> h{CertOpenSystemStoreW(0, L"ROOT")};

    if (!h)
        throw formatted_error("CertOpenSystemStore failed (error {})", GetLastError());

    while (true) {
        certctx = CertEnumCertificatesInStore(h.get(), certctx);
        if (!certctx)
            break;

        if (!(certctx->dwCertEncodingType & X509_ASN_ENCODING))
            continue;

        const unsigned char* cert = certctx->pbCertEncoded;

        unique_ptr<X509*, x509_closer> x509{d2i_X509(nullptr, &cert, certctx->cbCertEncoded)};

        if (!x509)
            continue;

        X509_STORE_add_cert(store, x509.get());
    }
}
#endif

namespace tds {
    int tds_ssl::ssl_read_cb(span<uint8_t> sp) {
        if (sp.empty())
            return 0;

        if (established) {
            auto& rb = ssl_recv_rb->get();
            auto to_copy = (uint32_t)min(sp.size(), rb.size());

            rb.read(span(sp.data(), to_copy));

            return (int)to_copy;
        } else {
            int copied = 0;

            if (!ssl_recv_buf.empty()) {
                auto to_copy = min(sp.size(), ssl_recv_buf.size());

                memcpy(sp.data(), ssl_recv_buf.data(), to_copy);

                vector<uint8_t> newbuf{ssl_recv_buf.data() + to_copy, ssl_recv_buf.data() + ssl_recv_buf.size()};

                ssl_recv_buf.swap(newbuf);

                if (sp.size() == to_copy)
                    return (int)sp.size();

                sp = sp.subspan(to_copy);

                copied = (int)to_copy;
            }

            enum tds_msg type;
            vector<uint8_t> payload;

            tds.sess.wait_for_msg(type, payload);

            if (type != tds_msg::prelogin)
                throw formatted_error("Received message type {}, expected prelogin", (int)type);

            auto to_copy = min(sp.size(), payload.size());

            memcpy(sp.data(), payload.data(), to_copy);
            copied += (int)to_copy;

            if (payload.size() > to_copy)
                ssl_recv_buf.insert(ssl_recv_buf.end(), payload.data() + to_copy, payload.data() + payload.size());

            return copied;
        }
    }

    int tds_ssl::ssl_write_cb(span<const uint8_t> sp) {
        if (established)
            ssl_send_buf.insert(ssl_send_buf.end(), sp.begin(), sp.end());
        else
            tds.sess.send_msg(tds_msg::prelogin, sp, false);

        return (int)sp.size();
    }

    long tds_ssl::ssl_ctrl_cb(int cmd, long, void*) {
        switch (cmd) {
            case BIO_CTRL_FLUSH:
                return 1;

            case BIO_C_DO_STATE_MACHINE: {
                auto ret = SSL_do_handshake(ssl.get());

                if (ret != 1) {
                    if (exception)
                        rethrow_exception(exception);

                    throw formatted_error("SSL_do_handshake failed (error {})", SSL_get_error(ssl.get(), ret));
                }

                return 1;
            }
        }

        return 0;
    }

    int tds_ssl::ssl_verify_cb(int preverify, X509_STORE_CTX* x509_ctx) {
        if (preverify)
            return preverify;

        int err = X509_STORE_CTX_get_error(x509_ctx);
        auto str = x509_err_to_string(err);

        throw formatted_error("Error verifying SSL certificate: {}", str);
    }

    tds_ssl::tds_ssl(tds_impl& tds) : tds(tds) {
        ctx.reset(SSL_CTX_new(SSLv23_method()));
        if (!ctx)
            throw ssl_error("SSL_CTX_new", ERR_get_error());

        if (tds.check_certificate) {
            if (!SSL_CTX_set_default_verify_paths(ctx.get()))
                throw ssl_error("SSL_CTX_set_default_verify_paths", ERR_get_error());

#ifdef _WIN32
            add_certs_to_store(SSL_CTX_get_cert_store(ctx.get()));
#endif

            SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_PEER, verify_callback);
        }

        SSL_CTX_set_options(ctx.get(), SSL_OP_ALL | SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION);

        meth.reset(BIO_meth_new(BIO_TYPE_MEM, "tdscpp"));
        if (!meth)
            throw ssl_error("BIO_meth_new", ERR_get_error());

        BIO_meth_set_read(meth.get(), ssl_bio_read);
        BIO_meth_set_write(meth.get(), ssl_bio_write);
        BIO_meth_set_ctrl(meth.get(), ssl_bio_ctrl);
        BIO_meth_set_destroy(meth.get(), [](BIO*) {
            return 1;
        });

        bio = BIO_new(meth.get());
        if (!bio)
            throw ssl_error("BIO_new", ERR_get_error());

        BIO_set_data(bio, this);

        ssl.reset(SSL_new(ctx.get()));
        if (!ssl) {
            BIO_free_all(bio);
            throw ssl_error("SSL_new", ERR_get_error());
        }

        if (!SSL_set_ex_data(ssl.get(), 0, this)) {
            BIO_free_all(bio);
            throw ssl_error("SSL_set_ex_data", ERR_get_error());
        }

        SSL_set_bio(ssl.get(), bio, bio);

        if (!tds.hostname.empty()) {
            if (!SSL_set1_host(ssl.get(), tds.hostname.c_str()))
                throw ssl_error("SSL_set1_host", ERR_get_error());

            if (!SSL_set_tlsext_host_name(ssl.get(), tds.hostname.c_str()))
                throw ssl_error("SSL_set_tlsext_host_name", ERR_get_error());
        }

        SSL_set_connect_state(ssl.get());

        SSL_connect(ssl.get());
        if (exception)
            rethrow_exception(exception);

        if (BIO_do_connect(bio) != 1) {
            if (exception)
                rethrow_exception(exception);

            throw ssl_error("BIO_do_connect", ERR_get_error());
        }

        if (BIO_do_handshake(bio) != 1) {
            if (exception)
                rethrow_exception(exception);

            throw ssl_error("BIO_do_handshake", ERR_get_error());
        }

        established = true;
    }

    vector<uint8_t> tds_ssl::enc(span<const uint8_t> sp) {
        ssl_send_buf.clear();

        while (!sp.empty()) {
            auto ret = SSL_write(ssl.get(), sp.data(), (int)sp.size());

            if (ret <= 0) {
                if (exception)
                    rethrow_exception(exception);

                throw formatted_error("SSL_write failed (error {})", SSL_get_error(ssl.get(), ret));
            }

            sp = sp.subspan((size_t)ret);
        }

        return ssl_send_buf;
    }

    vector<uint8_t> tds_ssl::dec(ringbuf& in_buf) {
        vector<uint8_t> out;
        uint8_t buf[4096];

        ssl_recv_rb = in_buf;

        while (!in_buf.empty()) {
            auto ret = SSL_read(ssl.get(), buf, sizeof(buf));

            if (ret <= 0) {
                if (exception)
                    rethrow_exception(exception);

                throw formatted_error("SSL_read failed (error {})", SSL_get_error(ssl.get(), ret));
            }

            out.insert(out.end(), buf, buf + ret);
        }

        return out;
    }
};
#elif defined(_WIN32)
namespace tds {
    tds_ssl::tds_ssl(tds_impl& tds) : tds(tds) {
        SECURITY_STATUS sec_status;
        SecBuffer outbuf;
        SecBufferDesc out;
        uint32_t context_attr;
        vector<uint8_t> outstr;
        SCHANNEL_CRED cred;

        memset(&cred, 0, sizeof(cred));

        cred.dwVersion = SCHANNEL_CRED_VERSION;
        cred.grbitEnabledProtocols = SP_PROT_TLS1_2_CLIENT;
        cred.dwFlags = tds.check_certificate ? SCH_CRED_AUTO_CRED_VALIDATION : SCH_CRED_MANUAL_CRED_VALIDATION;

        sec_status = AcquireCredentialsHandleW(nullptr, (LPWSTR)UNISP_NAME_W, SECPKG_CRED_OUTBOUND, nullptr, &cred,
                                               nullptr, nullptr, &cred_handle, nullptr);

        if (FAILED(sec_status))
            throw formatted_error("AcquireCredentialsHandle returned {}", (enum sec_error)sec_status);

        outbuf.cbBuffer = 0;
        outbuf.BufferType = SECBUFFER_TOKEN;
        outbuf.pvBuffer = nullptr;

        out.ulVersion = SECBUFFER_VERSION;
        out.cBuffers = 1;
        out.pBuffers = &outbuf;

        auto host = utf8_to_utf16(tds.hostname);

        sec_status = InitializeSecurityContextW(&cred_handle, nullptr, (SEC_WCHAR*)host.c_str(),
                                                ISC_REQ_ALLOCATE_MEMORY, 0, 0, nullptr, 0,
                                                &ctx_handle, &out, (ULONG*)&context_attr, nullptr);
        if (FAILED(sec_status)) {
            FreeCredentialsHandle(&cred_handle);
            throw formatted_error("InitializeSecurityContext returned {}", (enum sec_error)sec_status);
        }

        outstr.assign((uint8_t*)outbuf.pvBuffer, (uint8_t*)outbuf.pvBuffer + outbuf.cbBuffer);

        if (outbuf.pvBuffer)
            FreeContextBuffer(outbuf.pvBuffer);

        ctx_handle_set = true;

        while (sec_status == SEC_I_CONTINUE_NEEDED) {
            enum tds_msg type;
            vector<uint8_t> payload;
            SecBuffer inbuf;
            SecBufferDesc in;

            tds.sess.send_msg(tds_msg::prelogin, outstr, false);

            tds.sess.wait_for_msg(type, payload);

            if (type != tds_msg::prelogin) {
                FreeCredentialsHandle(&cred_handle);
                throw formatted_error("Received message type {}, expected prelogin", (int)type);
            }

            outbuf.cbBuffer = 0;
            outbuf.BufferType = SECBUFFER_TOKEN;
            outbuf.pvBuffer = nullptr;

            inbuf.cbBuffer = (long)payload.size();
            inbuf.BufferType = SECBUFFER_TOKEN;
            inbuf.pvBuffer = payload.data();

            in.ulVersion = SECBUFFER_VERSION;
            in.cBuffers = 1;
            in.pBuffers = &inbuf;

            sec_status = InitializeSecurityContextW(&cred_handle, &ctx_handle, nullptr,
                                                    ISC_REQ_ALLOCATE_MEMORY, 0, 0, &in, 0,
                                                    nullptr, &out, (ULONG*)&context_attr, nullptr);
            if (FAILED(sec_status)) {
                FreeCredentialsHandle(&cred_handle);
                throw formatted_error("InitializeSecurityContext returned {}", (enum sec_error)sec_status);
            }

            outstr.assign((uint8_t*)outbuf.pvBuffer, (uint8_t*)outbuf.pvBuffer + outbuf.cbBuffer);

            if (outbuf.pvBuffer)
                FreeContextBuffer(outbuf.pvBuffer);
        }

        if (sec_status != SEC_E_OK) {
            FreeCredentialsHandle(&cred_handle);
            throw formatted_error("InitializeSecurityContext returned unexpected status {}", (enum sec_error)sec_status);
        }

        sec_status = QueryContextAttributes(&ctx_handle, SECPKG_ATTR_STREAM_SIZES, &stream_sizes);
        if (FAILED(sec_status)) {
            FreeCredentialsHandle(&cred_handle);
            throw formatted_error("QueryContextAttributes(SECPKG_ATTR_STREAM_SIZES) returned {}", (enum sec_error)sec_status);
        }
    }

    tds_ssl::~tds_ssl() {
        if (ctx_handle_set)
            DeleteSecurityContext(&ctx_handle);

        FreeCredentialsHandle(&cred_handle);
    }

    vector<uint8_t> tds_ssl::enc(span<const uint8_t> sp) {
        SECURITY_STATUS sec_status;
        array<SecBuffer, 4> buf;
        SecBufferDesc bufdesc;
        vector<uint8_t> payload;

        memset(buf.data(), 0, sizeof(SecBuffer) * buf.size());

        payload.resize(stream_sizes.cbHeader + sp.size() + stream_sizes.cbTrailer);

        buf[0].BufferType = SECBUFFER_STREAM_HEADER;
        buf[0].pvBuffer = payload.data();
        buf[0].cbBuffer = stream_sizes.cbHeader;

        buf[1].cbBuffer = (long)sp.size();
        buf[1].BufferType = SECBUFFER_DATA;
        buf[1].pvBuffer = payload.data() + stream_sizes.cbHeader;

        buf[2].BufferType = SECBUFFER_STREAM_TRAILER;
        buf[2].pvBuffer = payload.data() + stream_sizes.cbHeader + sp.size();
        buf[2].cbBuffer = stream_sizes.cbTrailer;

        buf[3].BufferType = SECBUFFER_EMPTY;

        memcpy(buf[1].pvBuffer, sp.data(), sp.size());

        bufdesc.ulVersion = SECBUFFER_VERSION;
        bufdesc.cBuffers = (unsigned long)buf.size();
        bufdesc.pBuffers = buf.data();

        sec_status = EncryptMessage(&ctx_handle, 0, &bufdesc, 0);

        if (FAILED(sec_status))
            throw formatted_error("EncryptMessage returned {}", (enum sec_error)sec_status);

        payload.resize(buf[0].cbBuffer + buf[1].cbBuffer + buf[2].cbBuffer);

        return payload;
    }

    vector<uint8_t> tds_ssl::dec(ringbuf& in_buf) {
        SECURITY_STATUS sec_status;
        array<SecBuffer, 4> buf;
        SecBufferDesc bufdesc;
        vector<uint8_t> recvbuf;
        vector<uint8_t> ret;

        if (in_buf.empty())
            return {};

        recvbuf.resize(in_buf.size());
        in_buf.peek(recvbuf);

        while (!recvbuf.empty()) {
            bool found = false;

            memset(buf.data(), 0, sizeof(SecBuffer) * buf.size());
            buf[0].BufferType = SECBUFFER_DATA;
            buf[0].pvBuffer = recvbuf.data();
            buf[0].cbBuffer = (long)recvbuf.size();
            buf[1].BufferType = SECBUFFER_EMPTY;
            buf[2].BufferType = SECBUFFER_EMPTY;
            buf[3].BufferType = SECBUFFER_EMPTY;

            bufdesc.ulVersion = SECBUFFER_VERSION;
            bufdesc.cBuffers = buf.size();
            bufdesc.pBuffers = buf.data();

            sec_status = DecryptMessage(&ctx_handle, &bufdesc, 0, nullptr);

            if (sec_status == SEC_E_INCOMPLETE_MESSAGE && buf[0].BufferType == SECBUFFER_MISSING)
                return ret;

            if (FAILED(sec_status))
                throw formatted_error("DecryptMessage returned {}", (enum sec_error)sec_status);

            for (const auto& b : buf) {
                if (b.BufferType == SECBUFFER_DATA) {
                    ret.insert(ret.end(), (uint8_t*)b.pvBuffer, (uint8_t*)b.pvBuffer + b.cbBuffer);
                    found = true;
                    break;
                }
            }

            if (!found)
                throw runtime_error("DecryptMessage did not return a SECBUFFER_DATA buffer.");

            bool extra = false;
            for (const auto& b : buf) {
                if (b.BufferType == SECBUFFER_EXTRA) {
                    extra = true;

                    in_buf.discard((uint8_t*)b.pvBuffer - recvbuf.data());

                    vector<uint8_t> newbuf{(uint8_t*)b.pvBuffer, (uint8_t*)b.pvBuffer + b.cbBuffer};
                    recvbuf.swap(newbuf);

                    break;
                }
            }

            if (!extra)
                break;
        }

        in_buf.clear();

        return ret;
    }
};
#endif
