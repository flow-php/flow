use ext_php_rs::args::{Arg, ArgInfoTables};
use ext_php_rs::builders::{ClassBuilder, FunctionBuilder};
use ext_php_rs::error::Result;
use ext_php_rs::flags::{ClassFlags, DataType, MethodFlags};
use ext_php_rs::zend::ClassEntry;

use crate::interfaces::keep;

fn store(_ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
}

pub fn register() -> Result<()> {
    ClassBuilder::new("Flow\\Arrow\\RandomAccessFile")
        .flags(ClassFlags::Interface)
        .registration(store)
        .method(
            FunctionBuilder::new_abstract("read")
                .arg(Arg::new("length", DataType::Long))
                .arg(Arg::new("offset", DataType::Long))
                .returns(DataType::String, false, false),
            MethodFlags::Public | MethodFlags::Abstract,
        )
        .method(
            FunctionBuilder::new_abstract("size").returns(DataType::Long, false, true),
            MethodFlags::Public | MethodFlags::Abstract,
        )
        .register()?;

    Ok(())
}
