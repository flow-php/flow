use ext_php_rs::args::Arg;
use ext_php_rs::builders::{ClassBuilder, FunctionBuilder};
use ext_php_rs::error::Result;
use ext_php_rs::flags::{ClassFlags, DataType, MethodFlags};
use ext_php_rs::zend::ClassEntry;

fn noop(_ce: &'static mut ClassEntry) {}

pub fn register() -> Result<()> {
    ClassBuilder::new("Flow\\Arrow\\OutputStream")
        .flags(ClassFlags::Interface)
        .registration(noop)
        .method(
            FunctionBuilder::new_abstract("append")
                .arg(Arg::new("data", DataType::String))
                .returns(DataType::Object(Some("self")), false, false),
            MethodFlags::Public | MethodFlags::Abstract,
        )
        .register()?;

    Ok(())
}
