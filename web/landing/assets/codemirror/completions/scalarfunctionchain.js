/**
 * CodeMirror Completer for Flow PHP ScalarFunctionChain Methods
 *
 * ScalarFunctionChain methods: 121
 * ScalarFunctionChain-returning functions: 53
 *
 * This completer triggers after ScalarFunctionChain-returning DSL functions
 */

import { CompletionContext, snippet } from "@codemirror/autocomplete"

// DSL functions that return ScalarFunctionChain (have scalar_function_chain: true)
const scalarFunctionChainFunctions = [
    "col", "entry", "ref", "optional", "lit", "exists", "when", "array_get", "array_get_collection", "array_get_collection_first", "array_exists", "array_merge", "array_merge_collection", "array_key_rename", "array_keys_style_convert", "array_sort", "array_reverse", "now", "between", "to_date_time", "to_date", "date_time_format", "split", "combine", "concat", "concat_ws", "hash", "cast", "coalesce", "call", "array_unpack", "array_expand", "size", "uuid_v4", "uuid_v7", "ulid", "lower", "capitalize", "upper", "not", "to_timezone", "regex_replace", "regex_match_all", "regex_match", "regex", "regex_all", "sprintf", "sanitize", "round", "number_format", "greatest", "least", "match_cases"]

// ScalarFunctionChain methods
const scalarFunctionChainMethods = [
        {
        label: "and",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">and</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span>
                </div>
                            `
            return div
        },
        apply: snippet("and(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "andNot",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">andNot</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">All</span>
                </div>
                            `
            return div
        },
        apply: snippet("andNot(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "append",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">append</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$suffix</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Append</span>
                </div>
                            `
            return div
        },
        apply: snippet("append(" + "$" + "{" + "1:suffix" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayFilter",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayFilter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayFilter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Filters an array by removing all elements that matches passed value.<br>Applicable to all data structures that can be converted to an array:<br>   - json<br>   - list<br>   - map<br>   - structure.
                </div>
                            `
            return div
        },
        apply: snippet("arrayFilter(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayGet",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayGet</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGet</span>
                </div>
                            `
            return div
        },
        apply: snippet("arrayGet(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayGetCollection",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayGetCollection</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed> $keys
                </div>
                            `
            return div
        },
        apply: snippet("arrayGetCollection(" + "$" + "{" + "1:keys" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayGetCollectionFirst",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayGetCollectionFirst</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$keys</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayGetCollection</span>
                </div>
                            `
            return div
        },
        apply: snippet("arrayGetCollectionFirst(" + "$" + "{" + "1:keys" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayKeep",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayKeep</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeep</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Filters an array by keeping only elements that matches passed value.<br>Applicable to all data structures that can be converted to an array:<br>  - json<br>  - list<br>  - map<br>  - structure.
                </div>
                            `
            return div
        },
        apply: snippet("arrayKeep(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayKeys",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayKeys</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayKeys</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns all keys from an array, ignoring the values.<br>Applicable to all data structures that can be converted to an array:<br>  - json<br>  - list<br>  - map<br>  - structure.
                </div>
                            `
            return div
        },
        apply: snippet("arrayKeys()"),
        boost: 10
    },        {
        label: "arrayMerge",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayMerge</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMerge</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed> $ref
                </div>
                            `
            return div
        },
        apply: snippet("arrayMerge(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayMergeCollection",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayMergeCollection</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayMergeCollection</span>
                </div>
                            `
            return div
        },
        apply: snippet("arrayMergeCollection()"),
        boost: 10
    },        {
        label: "arrayPathExists",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayPathExists</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayPathExists</span>
                </div>
                            `
            return div
        },
        apply: snippet("arrayPathExists(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayReverse",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayReverse</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$preserveKeys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayReverse</span>
                </div>
                            `
            return div
        },
        apply: snippet("arrayReverse(" + "$" + "{" + "1:preserveKeys" + "}" + ")"),
        boost: 10
    },        {
        label: "arraySort",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arraySort</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|Sort|null</span> <span class=\"fn-param\">$sortFunction</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$recursive</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArraySort</span>
                </div>
                            `
            return div
        },
        apply: snippet("arraySort(" + "$" + "{" + "1:sortFunction" + "}" + ", " + "$" + "{" + "2:flags" + "}" + ", " + "$" + "{" + "3:recursive" + "}" + ")"),
        boost: 10
    },        {
        label: "arrayValues",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">arrayValues</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayValues</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns all values from an array, ignoring the keys.<br>Applicable to all data structures that can be converted to an array:<br>  - json<br>  - list<br>  - map<br>  - structure.
                </div>
                            `
            return div
        },
        apply: snippet("arrayValues()"),
        boost: 10
    },        {
        label: "ascii",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ascii</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Ascii</span>
                </div>
                            `
            return div
        },
        apply: snippet("ascii()"),
        boost: 10
    },        {
        label: "between",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">between</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$lowerBoundRef</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$upperBoundRef</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Boundary</span> <span class=\"fn-param\">$boundary</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Function\\Between\\Boundary::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Between</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param mixed|ScalarFunction $lowerBoundRef<br>@param mixed|ScalarFunction $upperBoundRef<br>@param Boundary|ScalarFunction $boundary
                </div>
                            `
            return div
        },
        apply: snippet("between(" + "$" + "{" + "1:lowerBoundRef" + "}" + ", " + "$" + "{" + "2:upperBoundRef" + "}" + ", " + "$" + "{" + "3:boundary" + "}" + ")"),
        boost: 10
    },        {
        label: "binaryLength",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">binaryLength</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">BinaryLength</span>
                </div>
                            `
            return div
        },
        apply: snippet("binaryLength()"),
        boost: 10
    },        {
        label: "call",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">call</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|callable</span> <span class=\"fn-param\">$callable</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">array</span> <span class=\"fn-param\">$arguments</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string|int</span> <span class=\"fn-param\">$refAlias</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Type</span> <span class=\"fn-param\">$returnType</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CallUserFunc</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed> $arguments<br>@param Type<mixed> $returnType
                </div>
                            `
            return div
        },
        apply: snippet("call(" + "$" + "{" + "1:callable" + "}" + ", " + "$" + "{" + "2:arguments" + "}" + ", " + "$" + "{" + "3:refAlias" + "}" + ", " + "$" + "{" + "4:returnType" + "}" + ")"),
        boost: 10
    },        {
        label: "capitalize",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">capitalize</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Capitalize</span>
                </div>
                            `
            return div
        },
        apply: snippet("capitalize()"),
        boost: 10
    },        {
        label: "cast",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type|string</span> <span class=\"fn-param\">$type</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Cast</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param string|Type<mixed> $type
                </div>
                            `
            return div
        },
        apply: snippet("cast(" + "$" + "{" + "1:type" + "}" + ")"),
        boost: 10
    },        {
        label: "chunk",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">chunk</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Chunk</span>
                </div>
                            `
            return div
        },
        apply: snippet("chunk(" + "$" + "{" + "1:size" + "}" + ")"),
        boost: 10
    },        {
        label: "coalesce",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">coalesce</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$params</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Coalesce</span>
                </div>
                            `
            return div
        },
        apply: snippet("coalesce(" + "$" + "{" + "1:params" + "}" + ")"),
        boost: 10
    },        {
        label: "codePointLength",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">codePointLength</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CodePointLength</span>
                </div>
                            `
            return div
        },
        apply: snippet("codePointLength()"),
        boost: 10
    },        {
        label: "collapseWhitespace",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">collapseWhitespace</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">CollapseWhitespace</span>
                </div>
                            `
            return div
        },
        apply: snippet("collapseWhitespace()"),
        boost: 10
    },        {
        label: "concat",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">concat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$params</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Concat</span>
                </div>
                            `
            return div
        },
        apply: snippet("concat(" + "$" + "{" + "1:params" + "}" + ")"),
        boost: 10
    },        {
        label: "concatWithSeparator",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">concatWithSeparator</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$params</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ConcatWithSeparator</span>
                </div>
                            `
            return div
        },
        apply: snippet("concatWithSeparator(" + "$" + "{" + "1:separator" + "}" + ", " + "$" + "{" + "2:params" + "}" + ")"),
        boost: 10
    },        {
        label: "contains",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">contains</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Contains</span>
                </div>
                            `
            return div
        },
        apply: snippet("contains(" + "$" + "{" + "1:needle" + "}" + ")"),
        boost: 10
    },        {
        label: "dateFormat",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dateFormat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DateTimeFormat</span>
                </div>
                            `
            return div
        },
        apply: snippet("dateFormat(" + "$" + "{" + "1:format" + "}" + ")"),
        boost: 10
    },        {
        label: "dateTimeFormat",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dateTimeFormat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d H:i:s&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DateTimeFormat</span>
                </div>
                            `
            return div
        },
        apply: snippet("dateTimeFormat(" + "$" + "{" + "1:format" + "}" + ")"),
        boost: 10
    },        {
        label: "divide",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">divide</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$scale</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|Rounding|null</span> <span class=\"fn-param\">$rounding</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Divide</span>
                </div>
                            `
            return div
        },
        apply: snippet("divide(" + "$" + "{" + "1:value" + "}" + ", " + "$" + "{" + "2:scale" + "}" + ", " + "$" + "{" + "3:rounding" + "}" + ")"),
        boost: 10
    },        {
        label: "domElementAttribute",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">domElementAttribute</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$attribute</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DOMElementAttributeValue</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated Use domElementAttributeValue instead
                </div>
                            `
            return div
        },
        apply: snippet("domElementAttribute(" + "$" + "{" + "1:attribute" + "}" + ")"),
        boost: 10
    },        {
        label: "domElementAttributesCount",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">domElementAttributesCount</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DOMElementAttributesCount</span>
                </div>
                            `
            return div
        },
        apply: snippet("domElementAttributesCount()"),
        boost: 10
    },        {
        label: "domElementAttributeValue",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">domElementAttributeValue</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$attribute</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DOMElementAttributeValue</span>
                </div>
                            `
            return div
        },
        apply: snippet("domElementAttributeValue(" + "$" + "{" + "1:attribute" + "}" + ")"),
        boost: 10
    },        {
        label: "domElementValue",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">domElementValue</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">DOMElementValue</span>
                </div>
                            `
            return div
        },
        apply: snippet("domElementValue()"),
        boost: 10
    },        {
        label: "endsWith",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">endsWith</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EndsWith</span>
                </div>
                            `
            return div
        },
        apply: snippet("endsWith(" + "$" + "{" + "1:needle" + "}" + ")"),
        boost: 10
    },        {
        label: "ensureEnd",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ensureEnd</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$suffix</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EnsureEnd</span>
                </div>
                            `
            return div
        },
        apply: snippet("ensureEnd(" + "$" + "{" + "1:suffix" + "}" + ")"),
        boost: 10
    },        {
        label: "ensureStart",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">ensureStart</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$prefix</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">EnsureStart</span>
                </div>
                            `
            return div
        },
        apply: snippet("ensureStart(" + "$" + "{" + "1:prefix" + "}" + ")"),
        boost: 10
    },        {
        label: "equals",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">equals</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Equals</span>
                </div>
                            `
            return div
        },
        apply: snippet("equals(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "exists",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">exists</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Exists</span>
                </div>
                            `
            return div
        },
        apply: snippet("exists()"),
        boost: 10
    },        {
        label: "expand",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">expand</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ArrayExpand</span> <span class=\"fn-param\">$expand</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Function\\ArrayExpand\\ArrayExpand::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayExpand</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Expands each value into entry, if there are more than one value, multiple rows will be created.<br>Array keys are ignored, only values are used to create new rows.<br>Before:<br>  +--+-------------------+<br>  |id|              array|<br>  +--+-------------------+<br>  | 1|{\"a\":1,\"b\":2,\"c\":3}|<br>  +--+-------------------+<br>After:<br>  +--+--------+<br>  |id|expanded|<br>  +--+--------+<br>  | 1|       1|<br>  | 1|       2|<br>  | 1|       3|<br>  +--+--------+
                </div>
                            `
            return div
        },
        apply: snippet("expand(" + "$" + "{" + "1:expand" + "}" + ")"),
        boost: 10
    },        {
        label: "greaterThan",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">greaterThan</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GreaterThan</span>
                </div>
                            `
            return div
        },
        apply: snippet("greaterThan(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "greaterThanEqual",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">greaterThanEqual</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GreaterThanEqual</span>
                </div>
                            `
            return div
        },
        apply: snippet("greaterThanEqual(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "hash",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">hash</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Algorithm</span> <span class=\"fn-param\">$algorithm</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Hash\\NativePHPHash::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Hash</span>
                </div>
                            `
            return div
        },
        apply: snippet("hash(" + "$" + "{" + "1:algorithm" + "}" + ")"),
        boost: 10
    },        {
        label: "htmlQuerySelector",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">htmlQuerySelector</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HTMLQuerySelector</span>
                </div>
                            `
            return div
        },
        apply: snippet("htmlQuerySelector(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "htmlQuerySelectorAll",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">htmlQuerySelectorAll</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$path</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">HTMLQuerySelectorAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("htmlQuerySelectorAll(" + "$" + "{" + "1:path" + "}" + ")"),
        boost: 10
    },        {
        label: "indexOf",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">indexOf</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$ignoreCase</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexOf</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns the index of given $needle in string.
                </div>
                            `
            return div
        },
        apply: snippet("indexOf(" + "$" + "{" + "1:needle" + "}" + ", " + "$" + "{" + "2:ignoreCase" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "indexOfLast",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">indexOfLast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$ignoreCase</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IndexOfLast</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns the last index of given $needle in string.
                </div>
                            `
            return div
        },
        apply: snippet("indexOfLast(" + "$" + "{" + "1:needle" + "}" + ", " + "$" + "{" + "2:ignoreCase" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "isEmpty",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isEmpty</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsEmpty</span>
                </div>
                            `
            return div
        },
        apply: snippet("isEmpty()"),
        boost: 10
    },        {
        label: "isEven",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isEven</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Equals</span>
                </div>
                            `
            return div
        },
        apply: snippet("isEven()"),
        boost: 10
    },        {
        label: "isFalse",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isFalse</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Same</span>
                </div>
                            `
            return div
        },
        apply: snippet("isFalse()"),
        boost: 10
    },        {
        label: "isIn",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isIn</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$haystack</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsIn</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed> $haystack
                </div>
                            `
            return div
        },
        apply: snippet("isIn(" + "$" + "{" + "1:haystack" + "}" + ")"),
        boost: 10
    },        {
        label: "isNotNull",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isNotNull</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsNotNull</span>
                </div>
                            `
            return div
        },
        apply: snippet("isNotNull()"),
        boost: 10
    },        {
        label: "isNotNumeric",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isNotNumeric</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsNotNumeric</span>
                </div>
                            `
            return div
        },
        apply: snippet("isNotNumeric()"),
        boost: 10
    },        {
        label: "isNull",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isNull</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsNull</span>
                </div>
                            `
            return div
        },
        apply: snippet("isNull()"),
        boost: 10
    },        {
        label: "isNumeric",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isNumeric</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsNumeric</span>
                </div>
                            `
            return div
        },
        apply: snippet("isNumeric()"),
        boost: 10
    },        {
        label: "isOdd",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isOdd</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NotEquals</span>
                </div>
                            `
            return div
        },
        apply: snippet("isOdd()"),
        boost: 10
    },        {
        label: "isTrue",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isTrue</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Same</span>
                </div>
                            `
            return div
        },
        apply: snippet("isTrue()"),
        boost: 10
    },        {
        label: "isType",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isType</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type|string</span> <span class=\"fn-param\">$types</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsType</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param string|Type<mixed> $types
                </div>
                            `
            return div
        },
        apply: snippet("isType(" + "$" + "{" + "1:types" + "}" + ")"),
        boost: 10
    },        {
        label: "isUtf8",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">isUtf8</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">IsUtf8</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Check string is utf8 and returns true or false.
                </div>
                            `
            return div
        },
        apply: snippet("isUtf8()"),
        boost: 10
    },        {
        label: "jsonDecode",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">jsonDecode</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">4194304</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonDecode</span>
                </div>
                            `
            return div
        },
        apply: snippet("jsonDecode(" + "$" + "{" + "1:flags" + "}" + ")"),
        boost: 10
    },        {
        label: "jsonEncode",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">jsonEncode</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">4194304</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">JsonEncode</span>
                </div>
                            `
            return div
        },
        apply: snippet("jsonEncode(" + "$" + "{" + "1:flags" + "}" + ")"),
        boost: 10
    },        {
        label: "lessThan",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lessThan</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LessThan</span>
                </div>
                            `
            return div
        },
        apply: snippet("lessThan(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "lessThanEqual",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lessThanEqual</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">LessThanEqual</span>
                </div>
                            `
            return div
        },
        apply: snippet("lessThanEqual(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "literal",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">literal</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Literal</span>
                </div>
                            `
            return div
        },
        apply: snippet("literal(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "lower",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">lower</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToLower</span>
                </div>
                            `
            return div
        },
        apply: snippet("lower()"),
        boost: 10
    },        {
        label: "minus",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">minus</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Minus</span>
                </div>
                            `
            return div
        },
        apply: snippet("minus(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "mod",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">mod</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Mod</span>
                </div>
                            `
            return div
        },
        apply: snippet("mod(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "modifyDateTime",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">modifyDateTime</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$modifier</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ModifyDateTime</span>
                </div>
                            `
            return div
        },
        apply: snippet("modifyDateTime(" + "$" + "{" + "1:modifier" + "}" + ")"),
        boost: 10
    },        {
        label: "multiply",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">multiply</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Multiply</span>
                </div>
                            `
            return div
        },
        apply: snippet("multiply(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "notEquals",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">notEquals</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NotEquals</span>
                </div>
                            `
            return div
        },
        apply: snippet("notEquals(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "notSame",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">notSame</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NotSame</span>
                </div>
                            `
            return div
        },
        apply: snippet("notSame(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "numberFormat",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">numberFormat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$decimals</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$decimalSeparator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;.&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$thousandsSeparator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;,&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">NumberFormat</span>
                </div>
                            `
            return div
        },
        apply: snippet("numberFormat(" + "$" + "{" + "1:decimals" + "}" + ", " + "$" + "{" + "2:decimalSeparator" + "}" + ", " + "$" + "{" + "3:thousandsSeparator" + "}" + ")"),
        boost: 10
    },        {
        label: "onEach",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">onEach</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$preserveKeys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">true</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">OnEach</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Execute a scalar function on each element of an array/list/map/structure entry.<br>In order to use this function, you need to provide a reference to the \"element\" that will be used in the function.<br>Example: $df->withEntry(\'array\', ref(\'array\')->onEach(ref(\'element\')->cast(type_string())))
                </div>
                            `
            return div
        },
        apply: snippet("onEach(" + "$" + "{" + "1:function" + "}" + ", " + "$" + "{" + "2:preserveKeys" + "}" + ")"),
        boost: 10
    },        {
        label: "or",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">or</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span>
                </div>
                            `
            return div
        },
        apply: snippet("or(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "orNot",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">orNot</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Any</span>
                </div>
                            `
            return div
        },
        apply: snippet("orNot(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "plus",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">plus</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int|float</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Plus</span>
                </div>
                            `
            return div
        },
        apply: snippet("plus(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "power",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">power</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Power</span>
                </div>
                            `
            return div
        },
        apply: snippet("power(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "prepend",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">prepend</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$prefix</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Prepend</span>
                </div>
                            `
            return div
        },
        apply: snippet("prepend(" + "$" + "{" + "1:prefix" + "}" + ")"),
        boost: 10
    },        {
        label: "regex",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regex</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Regex</span>
                </div>
                            `
            return div
        },
        apply: snippet("regex(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:flags" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regexAll",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regexAll</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("regexAll(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:flags" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regexMatch",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regexMatch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatch</span>
                </div>
                            `
            return div
        },
        apply: snippet("regexMatch(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:flags" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regexMatchAll",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regexMatchAll</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$flags</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$offset</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">0</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexMatchAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("regexMatchAll(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:flags" + "}" + ", " + "$" + "{" + "3:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "regexReplace",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">regexReplace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$replacement</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">RegexReplace</span>
                </div>
                            `
            return div
        },
        apply: snippet("regexReplace(" + "$" + "{" + "1:pattern" + "}" + ", " + "$" + "{" + "2:replacement" + "}" + ", " + "$" + "{" + "3:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "repeat",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">repeat</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$times</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Repeat</span>
                </div>
                            `
            return div
        },
        apply: snippet("repeat(" + "$" + "{" + "1:times" + "}" + ")"),
        boost: 10
    },        {
        label: "reverse",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reverse</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Reverse</span>
                </div>
                            `
            return div
        },
        apply: snippet("reverse()"),
        boost: 10
    },        {
        label: "round",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">round</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$precision</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">2</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$mode</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Round</span>
                </div>
                            `
            return div
        },
        apply: snippet("round(" + "$" + "{" + "1:precision" + "}" + ", " + "$" + "{" + "2:mode" + "}" + ")"),
        boost: 10
    },        {
        label: "same",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">same</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$value</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Same</span>
                </div>
                            `
            return div
        },
        apply: snippet("same(" + "$" + "{" + "1:value" + "}" + ")"),
        boost: 10
    },        {
        label: "sanitize",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sanitize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$placeholder</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;*&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int|null</span> <span class=\"fn-param\">$skipCharacters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sanitize</span>
                </div>
                            `
            return div
        },
        apply: snippet("sanitize(" + "$" + "{" + "1:placeholder" + "}" + ", " + "$" + "{" + "2:skipCharacters" + "}" + ")"),
        boost: 10
    },        {
        label: "size",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">size</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Size</span>
                </div>
                            `
            return div
        },
        apply: snippet("size()"),
        boost: 10
    },        {
        label: "slug",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">slug</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;-&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$locale</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array|null</span> <span class=\"fn-param\">$symbolsMap</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Slug</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param null|array<array-key, mixed> $symbolsMap
                </div>
                            `
            return div
        },
        apply: snippet("slug(" + "$" + "{" + "1:separator" + "}" + ", " + "$" + "{" + "2:locale" + "}" + ", " + "$" + "{" + "3:symbolsMap" + "}" + ")"),
        boost: 10
    },        {
        label: "split",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">split</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$separator</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">9223372036854775807</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Split</span>
                </div>
                            `
            return div
        },
        apply: snippet("split(" + "$" + "{" + "1:separator" + "}" + ", " + "$" + "{" + "2:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "sprintf",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sprintf</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string|int|float|null</span> <span class=\"fn-param\">$params</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Sprintf</span>
                </div>
                            `
            return div
        },
        apply: snippet("sprintf(" + "$" + "{" + "1:params" + "}" + ")"),
        boost: 10
    },        {
        label: "startsWith",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">startsWith</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StartsWith</span>
                </div>
                            `
            return div
        },
        apply: snippet("startsWith(" + "$" + "{" + "1:needle" + "}" + ")"),
        boost: 10
    },        {
        label: "stringAfter",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringAfter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$includeNeedle</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringAfter</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns the contents found after the first occurrence of the given string.
                </div>
                            `
            return div
        },
        apply: snippet("stringAfter(" + "$" + "{" + "1:needle" + "}" + ", " + "$" + "{" + "2:includeNeedle" + "}" + ")"),
        boost: 10
    },        {
        label: "stringAfterLast",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringAfterLast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$includeNeedle</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringAfterLast</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns the contents found after the last occurrence of the given string.
                </div>
                            `
            return div
        },
        apply: snippet("stringAfterLast(" + "$" + "{" + "1:needle" + "}" + ", " + "$" + "{" + "2:includeNeedle" + "}" + ")"),
        boost: 10
    },        {
        label: "stringBefore",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringBefore</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$includeNeedle</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringBefore</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns the contents found before the first occurrence of the given string.
                </div>
                            `
            return div
        },
        apply: snippet("stringBefore(" + "$" + "{" + "1:needle" + "}" + ", " + "$" + "{" + "2:includeNeedle" + "}" + ")"),
        boost: 10
    },        {
        label: "stringBeforeLast",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringBeforeLast</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$needle</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$includeNeedle</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringBeforeLast</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns the contents found before the last occurrence of the given string.
                </div>
                            `
            return div
        },
        apply: snippet("stringBeforeLast(" + "$" + "{" + "1:needle" + "}" + ", " + "$" + "{" + "2:includeNeedle" + "}" + ")"),
        boost: 10
    },        {
        label: "stringContainsAny",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringContainsAny</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$needles</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringContainsAny</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string>|ScalarFunction $needles
                </div>
                            `
            return div
        },
        apply: snippet("stringContainsAny(" + "$" + "{" + "1:needles" + "}" + ")"),
        boost: 10
    },        {
        label: "stringEqualsTo",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringEqualsTo</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$string</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringEqualsTo</span>
                </div>
                            `
            return div
        },
        apply: snippet("stringEqualsTo(" + "$" + "{" + "1:string" + "}" + ")"),
        boost: 10
    },        {
        label: "stringFold",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringFold</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringFold</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Returns a string that you can use in case-insensitive comparisons.
                </div>
                            `
            return div
        },
        apply: snippet("stringFold()"),
        boost: 10
    },        {
        label: "stringMatch",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringMatch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringMatch</span>
                </div>
                            `
            return div
        },
        apply: snippet("stringMatch(" + "$" + "{" + "1:pattern" + "}" + ")"),
        boost: 10
    },        {
        label: "stringMatchAll",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringMatchAll</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$pattern</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringMatchAll</span>
                </div>
                            `
            return div
        },
        apply: snippet("stringMatchAll(" + "$" + "{" + "1:pattern" + "}" + ")"),
        boost: 10
    },        {
        label: "stringNormalize",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringNormalize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$form</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">16</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringNormalize</span>
                </div>
                            `
            return div
        },
        apply: snippet("stringNormalize(" + "$" + "{" + "1:form" + "}" + ")"),
        boost: 10
    },        {
        label: "stringStyle",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringStyle</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|StringStyles|StringStyles|string</span> <span class=\"fn-param\">$style</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringStyle</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Covert string to a style from enum list, passed in parameter.<br>Can be string \"upper\" or StringStyles::UPPER for Upper (example).
                </div>
                            `
            return div
        },
        apply: snippet("stringStyle(" + "$" + "{" + "1:style" + "}" + ")"),
        boost: 10
    },        {
        label: "stringTitle",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringTitle</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$allWords</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringTitle</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Changes all graphemes/code points to \"title case\".
                </div>
                            `
            return div
        },
        apply: snippet("stringTitle(" + "$" + "{" + "1:allWords" + "}" + ")"),
        boost: 10
    },        {
        label: "stringWidth",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">stringWidth</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StringWidth</span>
                </div>
                            `
            return div
        },
        apply: snippet("stringWidth()"),
        boost: 10
    },        {
        label: "strPad",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">strPad</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$pad_string</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039; &#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">1</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrPad</span>
                </div>
                            `
            return div
        },
        apply: snippet("strPad(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:pad_string" + "}" + ", " + "$" + "{" + "3:type" + "}" + ")"),
        boost: 10
    },        {
        label: "strPadBoth",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">strPadBoth</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$pad_string</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039; &#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrPad</span>
                </div>
                            `
            return div
        },
        apply: snippet("strPadBoth(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:pad_string" + "}" + ")"),
        boost: 10
    },        {
        label: "strPadLeft",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">strPadLeft</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$pad_string</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039; &#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrPad</span>
                </div>
                            `
            return div
        },
        apply: snippet("strPadLeft(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:pad_string" + "}" + ")"),
        boost: 10
    },        {
        label: "strPadRight",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">strPadRight</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$pad_string</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039; &#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrPad</span>
                </div>
                            `
            return div
        },
        apply: snippet("strPadRight(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:pad_string" + "}" + ")"),
        boost: 10
    },        {
        label: "strReplace",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">strReplace</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array|string</span> <span class=\"fn-param\">$search</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|array|string</span> <span class=\"fn-param\">$replace</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">StrReplace</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<string>|ScalarFunction|string $search<br>@param array<string>|ScalarFunction|string $replace
                </div>
                            `
            return div
        },
        apply: snippet("strReplace(" + "$" + "{" + "1:search" + "}" + ", " + "$" + "{" + "2:replace" + "}" + ")"),
        boost: 10
    },        {
        label: "toDate",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">toDate</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d\\\\TH:i:sP&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDate</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param ScalarFunction|string $format - current format of the date that will be used to create DateTimeImmutable instance
                </div>
                            `
            return div
        },
        apply: snippet("toDate(" + "$" + "{" + "1:format" + "}" + ", " + "$" + "{" + "2:timeZone" + "}" + ")"),
        boost: 10
    },        {
        label: "toDateTime",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">toDateTime</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$format</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;Y-m-d H:i:s&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|DateTimeZone</span> <span class=\"fn-param\">$timeZone</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">DateTimeZone::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToDateTime</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param ScalarFunction|string $format - current format of the date that will be used to create DateTimeImmutable instance<br>@param \\DateTimeZone|ScalarFunction $timeZone
                </div>
                            `
            return div
        },
        apply: snippet("toDateTime(" + "$" + "{" + "1:format" + "}" + ", " + "$" + "{" + "2:timeZone" + "}" + ")"),
        boost: 10
    },        {
        label: "trim",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">trim</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Type</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Function\\Trim\\Type::...</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$characters</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039; \\t\\n\\r\\0&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Trim</span>
                </div>
                            `
            return div
        },
        apply: snippet("trim(" + "$" + "{" + "1:type" + "}" + ", " + "$" + "{" + "2:characters" + "}" + ")"),
        boost: 10
    },        {
        label: "truncate",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">truncate</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$length</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$ellipsis</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;...&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Truncate</span>
                </div>
                            `
            return div
        },
        apply: snippet("truncate(" + "$" + "{" + "1:length" + "}" + ", " + "$" + "{" + "2:ellipsis" + "}" + ")"),
        boost: 10
    },        {
        label: "unicodeLength",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">unicodeLength</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">UnicodeLength</span>
                </div>
                            `
            return div
        },
        apply: snippet("unicodeLength()"),
        boost: 10
    },        {
        label: "unpack",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">unpack</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|array</span> <span class=\"fn-param\">$skipKeys</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">[]</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string|null</span> <span class=\"fn-param\">$entryPrefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ArrayUnpack</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param array<array-key, mixed> $skipKeys
                </div>
                            `
            return div
        },
        apply: snippet("unpack(" + "$" + "{" + "1:skipKeys" + "}" + ", " + "$" + "{" + "2:entryPrefix" + "}" + ")"),
        boost: 10
    },        {
        label: "upper",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">upper</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">ToUpper</span>
                </div>
                            `
            return div
        },
        apply: snippet("upper()"),
        boost: 10
    },        {
        label: "wordwrap",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">wordwrap</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction|int</span> <span class=\"fn-param\">$width</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|string</span> <span class=\"fn-param\">$break</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;\\n&#039;</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|bool</span> <span class=\"fn-param\">$cut</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Wordwrap</span>
                </div>
                            `
            return div
        },
        apply: snippet("wordwrap(" + "$" + "{" + "1:width" + "}" + ", " + "$" + "{" + "2:break" + "}" + ", " + "$" + "{" + "3:cut" + "}" + ")"),
        boost: 10
    },        {
        label: "xpath",
        type: "method",
        detail: "Flow\\\\ETL\\\\Function\\\\ScalarFunctionChain",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">xpath</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$string</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">XPath</span>
                </div>
                            `
            return div
        },
        apply: snippet("xpath(" + "$" + "{" + "1:string" + "}" + ")"),
        boost: 10
    }    ]

/**
 * ScalarFunctionChain method completion source for CodeMirror
 * @param {CompletionContext} context
 * @returns {CompletionResult|null}
 */
export function scalarFunctionChainCompletions(context) {
    // Get text before cursor (potentially across multiple lines)
    // Look back up to 2000 characters to find the pattern
    const maxLookback = 2000
    const docText = context.state.doc.toString()
    const startPos = Math.max(0, context.pos - maxLookback)
    const textBefore = docText.slice(startPos, context.pos)

    // Check if we're directly after -> (method chaining context)
    if (!new RegExp('->\\w*$').test(textBefore)) {
        return null
    }

    // Check if we're after a ScalarFunctionChain-returning function
    // Pattern: any of the DSL functions with scalar_function_chain: true
    if (scalarFunctionChainFunctions.length === 0) {
        return null
    }

    // Find all matching functions and check if any are at top level with ->
    const functionPattern = new RegExp('\\b(' + scalarFunctionChainFunctions.join('|') + ')\\s*\\(', 'g')
    let matches = []
    let match

    while ((match = functionPattern.exec(textBefore)) !== null) {
        matches.push({ name: match[1], index: match.index, endOfName: match.index + match[0].length })
    }

    // Walk backwards from cursor tracking parenthesis depth
    let depth = 0
    let i = textBefore.length - 1

    // Skip back past the -> and any word being typed
    while (i >= 0 && /[\w>-]/.test(textBefore[i])) {
        i--
    }

    // Now count parentheses going backwards
    while (i >= 0) {
        if (textBefore[i] === ')') depth++
        else if (textBefore[i] === '(') {
            depth--
            // If we're back to depth 0, check if this ( belongs to a ScalarFunctionChain function
            if (depth === 0) {
                // Look backwards to find the function name
                let funcEnd = i
                while (funcEnd > 0 && /\s/.test(textBefore[funcEnd - 1])) {
                    funcEnd--
                }
                let funcStart = funcEnd
                while (funcStart > 0 && /\w/.test(textBefore[funcStart - 1])) {
                    funcStart--
                }
                const funcName = textBefore.slice(funcStart, funcEnd)

                // Check if this is a ScalarFunctionChain-returning function
                if (scalarFunctionChainFunctions.includes(funcName)) {
                    // This is it! We're directly after this function call
                    return continueWithCompletions()
                }
                // If not, we're inside some other call
                return null
            }
        }
        i--
    }

    return null

    function continueWithCompletions() {
        // Match word being typed (method name after ->)
        const word = context.matchBefore(/\w*/)

        // If no word and not explicit, don't show completions
        if (!word && !context.explicit) {
            return null
        }

        // Filter methods based on what's being typed
        const prefix = word ? word.text.toLowerCase() : ''
        const options = scalarFunctionChainMethods.filter(method =>
            !prefix || method.label.toLowerCase().startsWith(prefix)
        )

        return {
            from: word ? word.from : context.pos,
            options: options,
            validFor: new RegExp('^\\w*$')  // Reuse while typing word characters
        }
    }
}
