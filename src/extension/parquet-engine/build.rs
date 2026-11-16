fn main() {
    emit_php_version_cfg();
}

/// The `php84`/`php85` cfgs ext-php-rs sets for itself, for the same PHP (`PHP` first, then `PATH`): the extension
/// binary is bound to the PHP minor it is built against, so version gates are compile-time.
fn emit_php_version_cfg() {
    use ext_php_rs_build::{
        emit_check_cfg, emit_php_cfg_flags, emit_rerun_if_env_changed, find_php, ApiVersion, PHPInfo,
    };

    emit_rerun_if_env_changed();
    emit_check_cfg();

    let php = find_php().expect("cannot find the php executable");
    let info = PHPInfo::get(&php).expect("cannot read php -i");
    let version: ApiVersion = info
        .zend_version()
        .expect("cannot read the Zend API version")
        .try_into()
        .expect("unsupported Zend API version");

    emit_php_cfg_flags(version);
}
