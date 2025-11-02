#include "sapi/embed/php_embed.h"
#include "Zend/zend_exceptions.h"
#include "Zend/zend_interfaces.h"
#include <emscripten.h>
#include <stdlib.h>

// From Zend/zend_exceptions.c for php 7.3
#if PHP_MAJOR_VERSION >= 8
#define GET_PROPERTY_SILENT(object, id) \
	zend_read_property_ex(i_get_exception_base(object), (Z_OBJ_P(object)), ZSTR_KNOWN(id), 1, &rv)
#else
#define GET_PROPERTY_SILENT(object, id) \
	zend_read_property_ex(i_get_exception_base(object), (object), ZSTR_KNOWN(id), 1, &rv)
#endif

// Source: php-src/sapi/php_cli.c
static inline zend_class_entry *i_get_exception_base(zval *object) /* {{{ */
{
	return instanceof_function(Z_OBJCE_P(object), zend_ce_exception) ? zend_ce_exception : zend_ce_error;
}
static void pib_cli_register_file_handles(void) /* {{{ */
{
    php_stream /* *s_in, */ *s_out, *s_err;
    php_stream_context /* *sc_in=NULL, */ *sc_out=NULL, *sc_err=NULL;
    zend_constant /* ic, */ oc, ec;

    // s_in  = php_stream_open_wrapper_ex("php://stdin",  "rb", 0, NULL, sc_in);
    s_out = php_stream_open_wrapper_ex("php://stdout", "wb", 0, NULL, sc_out);
    s_err = php_stream_open_wrapper_ex("php://stderr", "wb", 0, NULL, sc_err);

    if (/* s_in == NULL || */ s_out==NULL || s_err==NULL) {
        // if (s_in) php_stream_close(s_in);
        if (s_out) php_stream_close(s_out);
        if (s_err) php_stream_close(s_err);
        return;
    }

    // TODO: Support s_in
    // s_in_process = s_in;

    // TODO: Set up an empty stream instead for STDIN
    // php_stream_to_zval(s_in,  &ic.value);
    php_stream_to_zval(s_out, &oc.value);
    php_stream_to_zval(s_err, &ec.value);

    /*
    ZEND_CONSTANT_SET_FLAGS(&ic, CONST_CS, 0);
    ic.name = zend_string_init_interned("STDIN", sizeof("STDIN")-1, 0);
    zend_register_constant(&ic);
    */

    ZEND_CONSTANT_SET_FLAGS(&oc, CONST_CS, 0);
    oc.name = zend_string_init_interned("STDOUT", sizeof("STDOUT")-1, 0);
    zend_register_constant(&oc);

    ZEND_CONSTANT_SET_FLAGS(&ec, CONST_CS, 0);
    ec.name = zend_string_init_interned("STDERR", sizeof("STDERR")-1, 0);
    zend_register_constant(&ec);
}
/* }}} */

// Based on void zend_exception_error
static void pib_report_exception(zend_object *ex) {
    // printf("exception=%llx\n", (long long)ex);
    zval exception;

    ZVAL_OBJ(&exception, ex);
    zend_class_entry *ce_exception = Z_OBJCE(exception);

    // Cast to string and report it.
    // zend_exception_error(ex, E_ERROR);
    if (ce_exception) {
        zval rv;
		zend_string *message = zval_get_string(GET_PROPERTY_SILENT(&exception, ZEND_STR_MESSAGE));
        fprintf(stderr, "Uncaught throwable '%s': %s\n", ZSTR_VAL(ce_exception->name), ZSTR_VAL(message));
        zend_string_release(message);
		zend_string *file = zval_get_string(GET_PROPERTY_SILENT(&exception, ZEND_STR_FILE));
		zend_long line = zval_get_long(GET_PROPERTY_SILENT(&exception, ZEND_STR_LINE));
        fprintf(stderr, "At %s:%d\n", ZSTR_VAL(file), line);
        zend_string_release(file);
        /*
        // Can't get this to work at the end of execution.
        if (instanceof_function(ce_exception, zend_ce_throwable)) {
            zval tmp;
            // TODO handle uncaught exception caused by __toString()
            zend_call_method_with_0_params(&exception, ce_exception, &ce_exception->__tostring, "__tostring", &tmp);
            if (Z_TYPE(tmp) == IS_STRING) {
                fprintf(stderr, "%s", Z_STRVAL(tmp));
            } else {
                fprintf(stderr, "Calling __toString failed\n");
            }
            zval_ptr_dtor(&tmp);
        }
        */
    }
}

// Based on code by https://github.com/oraoto/pib with modifications.
int EMSCRIPTEN_KEEPALIVE pib_eval(char *code) {
    int ret = 0;
    // USE_ZEND_ALLOC prevents using fast shutdown.
    // putenv("USE_ZEND_ALLOC=0");
    php_embed_init(0, NULL);
    pib_cli_register_file_handles();

    // Show fatal E_COMPILE_ERRORs and other errors properly (startup errors are normally hidden)
    PG(display_startup_errors)=1;
    PG(during_request_startup)=0;

    zend_first_try {
        ret = zend_eval_string(code, NULL, "PIB");

        // If there was an uncaught error/exception, then report it.
        zend_object *ex = EG(exception);
        if (ex != NULL) {
            pib_report_exception(ex);
        }
    } zend_catch {
        zend_object *ex = EG(exception);
        if (ex != NULL) {
            EG(exception) = NULL;
            pib_report_exception(ex);
            EG(exception) = ex;
        }
        ret = EG(exit_status);
    } zend_end_try();
    php_embed_shutdown();
    fflush(stdout);
    fflush(stderr);
    return ret;
}

int EMSCRIPTEN_KEEPALIVE pib_force_exit() {
    emscripten_force_exit(0);
    return 0;
}
