import * as bindings from "../wasm/mago/mago_wasm_bg.js"

let loading = null

export function mago(wasmUrl) {
    loading ??= fetch(wasmUrl)
        .then(response => response.arrayBuffer())
        .then(bytes => WebAssembly.instantiate(bytes, { "./mago_wasm_bg.js": bindings }))
        .then(({ instance }) => {
            bindings.__wbg_set_wasm(instance.exports)
            return bindings
        })
        .catch(error => {
            loading = null
            throw error
        })

    return loading
}
