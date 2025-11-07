/**
 * CodeMirror Completer for Flow PHP DataFrame Methods
 *
 * DataFrame methods: 61
 * DataFrame-returning methods from classes: 3
 *
 * This completer triggers after DataFrame-returning methods
 */

import { CompletionContext, snippet } from "@codemirror/autocomplete"

// Map of DataFrame-returning methods grouped by class
const dataframeReturningMethods = {"flow":["extract","from","process","read"],"dataframe":["aggregate","autoCast","batchBy","batchSize","cache","collect","collectRefs","constrain","crossJoin","drop","dropDuplicates","dropPartitions","duplicateRow","filter","filterPartitions","filters","join","joinEach","limit","load","map","match","mode","offset","onError","partitionBy","pivot","rename","renameAll","renameAllLowerCase","renameAllStyle","renameAllUpperCase","renameAllUpperCaseFirst","renameAllUpperCaseWord","renameEach","reorderEntries","rows","saveMode","select","sortBy","transform","until","validate","void","with","withEntries","withEntry","write"],"groupeddataframe":["aggregate"]};

// DataFrame methods
const dataframeMethods = [
        {
        label: "aggregate",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">aggregate</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">AggregatingFunction</span> <span class=\"fn-param\">$aggregations</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("aggregate(" + "$" + "{" + "1:aggregations" + "}" + ")"),
        boost: 10
    },        {
        label: "autoCast",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">autoCast</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                            `
            return div
        },
        apply: snippet("autoCast()"),
        boost: 10
    },        {
        label: "batchBy",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batchBy</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$column</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$minSize</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Merge/Split Rows yielded by Extractor into batches but keep those with common value in given column together.<br>This works properly only on sorted datasets.<br>When minSize is not provided, batches will be created only when there is a change in value of the column.<br>When minSize is provided, batches will be created only when there is a change in value of the column or<br>when there are at least minSize rows in the batch.<br>@param Reference|string $column - column to group by (all rows with same value stay together)<br>@param null|int<1, max> $minSize - optional minimum rows per batch for efficiency<br>@lazy<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("batchBy(" + "$" + "{" + "1:column" + "}" + ", " + "$" + "{" + "2:minSize" + "}" + ")"),
        boost: 10
    },        {
        label: "batchSize",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">batchSize</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$size</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Merge/Split Rows yielded by Extractor into batches of given size.<br>For example, when Extractor is yielding one row at time, this method will merge them into batches of given size<br>before passing them to the next pipeline element.<br>Similarly when Extractor is yielding batches of rows, this method will split them into smaller batches of given<br>size.<br>In order to merge all Rows into a single batch use DataFrame::collect() method or set size to -1 or 0.<br>@param int<1, max> $size<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("batchSize(" + "$" + "{" + "1:size" + "}" + ")"),
        boost: 10
    },        {
        label: "cache",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">cache</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$id</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int</span> <span class=\"fn-param\">$cacheBatchSize</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Start processing rows up to this moment and put each instance of Rows<br>into previously defined cache.<br>Cache type can be set through ConfigBuilder.<br>By default everything is cached in system tmp dir.<br>Important: cache batch size might significantly improve performance when processing large amount of rows.<br>Larger batch size will increase memory consumption but will reduce number of IO operations.<br>When not set, the batch size is taken from the last DataFrame::batchSize() call.<br>@lazy<br>@param null|string $id<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("cache(" + "$" + "{" + "1:id" + "}" + ", " + "$" + "{" + "2:cacheBatchSize" + "}" + ")"),
        boost: 10
    },        {
        label: "collect",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">collect</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Before transforming rows, collect them and merge into single Rows instance.<br>This might lead to memory issues when processing large amount of rows, use with caution.<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("collect()"),
        boost: 10
    },        {
        label: "collectRefs",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">collectRefs</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">References</span> <span class=\"fn-param\">$references</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    This method allows to collect references to all entries used in this pipeline.<br>\`\`\`php<br>(new Flow())<br>  ->read(From::chain())<br>  ->collectRefs($refs = refs())<br>  ->run();<br>\`\`\`<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("collectRefs(" + "$" + "{" + "1:references" + "}" + ")"),
        boost: 10
    },        {
        label: "constrain",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">constrain</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Constraint</span> <span class=\"fn-param\">$constraint</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Constraint</span> <span class=\"fn-param\">$constraints</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                            `
            return div
        },
        apply: snippet("constrain(" + "$" + "{" + "1:constraint" + "}" + ", " + "$" + "{" + "2:constraints" + "}" + ")"),
        boost: 10
    },        {
        label: "count",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">count</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">int</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @trigger<br>Return total count of rows processed by this pipeline.
                </div>
                            `
            return div
        },
        apply: snippet("count()"),
        boost: 10
    },        {
        label: "crossJoin",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">crossJoin</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">self</span> <span class=\"fn-param\">$dataFrame</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$prefix</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">&#039;&#039;</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("crossJoin(" + "$" + "{" + "1:dataFrame" + "}" + ", " + "$" + "{" + "2:prefix" + "}" + ")"),
        boost: 10
    },        {
        label: "display",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">display</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">string</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param int $limit maximum numbers of rows to display<br>@param bool|int $truncate false or if set to 0 columns are not truncated, otherwise default truncate to 20<br>                          characters<br>@param Formatter $formatter<br>@trigger<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("display(" + "$" + "{" + "1:limit" + "}" + ", " + "$" + "{" + "2:truncate" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ")"),
        boost: 10
    },        {
        label: "drop",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">drop</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Drop given entries.<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("drop(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "dropDuplicates",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dropDuplicates</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Reference|string ...$entries<br>@lazy<br>@return $this
                </div>
                            `
            return div
        },
        apply: snippet("dropDuplicates(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "dropPartitions",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">dropPartitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">bool</span> <span class=\"fn-param\">$dropPartitionColumns</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Drop all partitions from Rows, additionally when $dropPartitionColumns is set to true, partition columns are<br>also removed.<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("dropPartitions(" + "$" + "{" + "1:dropPartitionColumns" + "}" + ")"),
        boost: 10
    },        {
        label: "duplicateRow",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">duplicateRow</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">mixed</span> <span class=\"fn-param\">$condition</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">WithEntry</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                            `
            return div
        },
        apply: snippet("duplicateRow(" + "$" + "{" + "1:condition" + "}" + ", " + "$" + "{" + "2:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "fetch",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">fetch</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Rows</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Be aware that fetch is not memory safe and will load all rows into memory.<br>If you want to safely iterate over Rows use oe of the following methods:.<br>DataFrame::get() : \\Generator<br>DataFrame::getAsArray() : \\Generator<br>DataFrame::getEach() : \\Generator<br>DataFrame::getEachAsArray() : \\Generator<br>@trigger<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("fetch(" + "$" + "{" + "1:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "filter",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">filter</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("filter(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "filterPartitions",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">filterPartitions</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Filter|ScalarFunction</span> <span class=\"fn-param\">$filter</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@throws RuntimeException
                </div>
                            `
            return div
        },
        apply: snippet("filterPartitions(" + "$" + "{" + "1:filter" + "}" + ")"),
        boost: 10
    },        {
        label: "filters",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">filters</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$functions</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@param array<ScalarFunction> $functions
                </div>
                            `
            return div
        },
        apply: snippet("filters(" + "$" + "{" + "1:functions" + "}" + ")"),
        boost: 10
    },        {
        label: "forEach",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">forEach</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">callable</span> <span class=\"fn-param\">$callback</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">void</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @trigger<br>@param null|callable(Rows $rows) : void $callback
                </div>
                            `
            return div
        },
        apply: snippet("forEach(" + "$" + "{" + "1:callback" + "}" + ")"),
        boost: 10
    },        {
        label: "get",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">get</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Yields each row as an instance of Rows.<br>@trigger<br>@return \\Generator<Rows>
                </div>
                            `
            return div
        },
        apply: snippet("get()"),
        boost: 10
    },        {
        label: "getAsArray",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">getAsArray</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Yields each row as an array.<br>@trigger<br>@return \\Generator<array<array<mixed>>>
                </div>
                            `
            return div
        },
        apply: snippet("getAsArray()"),
        boost: 10
    },        {
        label: "getEach",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">getEach</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Yield each row as an instance of Row.<br>@trigger<br>@return \\Generator<Row>
                </div>
                            `
            return div
        },
        apply: snippet("getEach()"),
        boost: 10
    },        {
        label: "getEachAsArray",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">getEachAsArray</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Generator</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Yield each row as an array.<br>@trigger<br>@return \\Generator<array<mixed>>
                </div>
                            `
            return div
        },
        apply: snippet("getEachAsArray()"),
        boost: 10
    },        {
        label: "groupBy",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">groupBy</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">GroupedDataFrame</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("groupBy(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "join",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">join</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">self</span> <span class=\"fn-param\">$dataFrame</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$on</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Join|string</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Join\\Join::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("join(" + "$" + "{" + "1:dataFrame" + "}" + ", " + "$" + "{" + "2:on" + "}" + ", " + "$" + "{" + "3:type" + "}" + ")"),
        boost: 10
    },        {
        label: "joinEach",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">joinEach</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">DataFrameFactory</span> <span class=\"fn-param\">$factory</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Expression</span> <span class=\"fn-param\">$on</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Join|string</span> <span class=\"fn-param\">$type</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Join\\Join::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@psalm-param string|Join $type
                </div>
                            `
            return div
        },
        apply: snippet("joinEach(" + "$" + "{" + "1:factory" + "}" + ", " + "$" + "{" + "2:on" + "}" + ", " + "$" + "{" + "3:type" + "}" + ")"),
        boost: 10
    },        {
        label: "limit",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">limit</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("limit(" + "$" + "{" + "1:limit" + "}" + ")"),
        boost: 10
    },        {
        label: "load",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">load</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("load(" + "$" + "{" + "1:loader" + "}" + ")"),
        boost: 10
    },        {
        label: "map",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">map</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">callable</span> <span class=\"fn-param\">$callback</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@param callable(Row $row) : Row $callback
                </div>
                            `
            return div
        },
        apply: snippet("map(" + "$" + "{" + "1:callback" + "}" + ")"),
        boost: 10
    },        {
        label: "match",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">match</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaValidator</span> <span class=\"fn-param\">$validator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@param null|SchemaValidator $validator - when null, StrictValidator gets initialized
                </div>
                            `
            return div
        },
        apply: snippet("match(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:validator" + "}" + ")"),
        boost: 10
    },        {
        label: "mode",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">mode</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SaveMode</span> <span class=\"fn-param\">$mode</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    SaveMode defines how Flow should behave when writing to a file/files that already exists.<br>For more details please see SaveMode enum.<br>@param SaveMode $mode<br>@lazy<br>@return $this
                </div>
                            `
            return div
        },
        apply: snippet("mode(" + "$" + "{" + "1:mode" + "}" + ")"),
        boost: 10
    },        {
        label: "offset",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">offset</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$offset</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Skip given number of rows from the beginning of the dataset.<br>When $offset is null, nothing happens (no rows are skipped).<br>Performance Note: DataFrame must iterate through and process all skipped rows<br>to reach the offset position. For large offsets, this can impact performance<br>as the data source still needs to be read and processed up to the offset point.<br>@param ?int<0, max> $offset<br>@lazy<br>@throws InvalidArgumentException
                </div>
                            `
            return div
        },
        apply: snippet("offset(" + "$" + "{" + "1:offset" + "}" + ")"),
        boost: 10
    },        {
        label: "onError",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">onError</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ErrorHandler</span> <span class=\"fn-param\">$handler</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("onError(" + "$" + "{" + "1:handler" + "}" + ")"),
        boost: 10
    },        {
        label: "partitionBy",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">partitionBy</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("partitionBy(" + "$" + "{" + "1:entry" + "}" + ", " + "$" + "{" + "2:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "pivot",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">pivot</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference</span> <span class=\"fn-param\">$ref</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                            `
            return div
        },
        apply: snippet("pivot(" + "$" + "{" + "1:ref" + "}" + ")"),
        boost: 10
    },        {
        label: "printRows",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">printRows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">int|bool</span> <span class=\"fn-param\">$truncate</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Formatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Formatter\\AsciiTableFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">void</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @trigger
                </div>
                            `
            return div
        },
        apply: snippet("printRows(" + "$" + "{" + "1:limit" + "}" + ", " + "$" + "{" + "2:truncate" + "}" + ", " + "$" + "{" + "3:formatter" + "}" + ")"),
        boost: 10
    },        {
        label: "printSchema",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">printSchema</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">int</span> <span class=\"fn-param\">$limit</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">20</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaFormatter</span> <span class=\"fn-param\">$formatter</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Row\\Formatter\\ASCIISchemaFormatter::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">void</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @trigger
                </div>
                            `
            return div
        },
        apply: snippet("printSchema(" + "$" + "{" + "1:limit" + "}" + ", " + "$" + "{" + "2:formatter" + "}" + ")"),
        boost: 10
    },        {
        label: "rename",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rename</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$from</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$to</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("rename(" + "$" + "{" + "1:from" + "}" + ", " + "$" + "{" + "2:to" + "}" + ")"),
        boost: 10
    },        {
        label: "renameAll",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">renameAll</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">string</span> <span class=\"fn-param\">$search</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">string</span> <span class=\"fn-param\">$replace</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>Iterate over all entry names and replace the given search string with replace string.<br>@deprecated use DataFrame::renameEach() with a RenameReplaceStrategy
                </div>
                            `
            return div
        },
        apply: snippet("renameAll(" + "$" + "{" + "1:search" + "}" + ", " + "$" + "{" + "2:replace" + "}" + ")"),
        boost: 10
    },        {
        label: "renameAllLowerCase",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">renameAllLowerCase</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@deprecated use DataFrame::renameEach() with a selected StringStyles
                </div>
                            `
            return div
        },
        apply: snippet("renameAllLowerCase()"),
        boost: 10
    },        {
        label: "renameAllStyle",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">renameAllStyle</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">StringStyles|StringStyles|string</span> <span class=\"fn-param\">$style</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>Rename all entries to a given style.<br>Please look into \\Flow\\ETL\\Function\\StyleConverter\\StringStyles class for all available styles.<br>@deprecated use DataFrame::renameEach() with a selected Style
                </div>
                            `
            return div
        },
        apply: snippet("renameAllStyle(" + "$" + "{" + "1:style" + "}" + ")"),
        boost: 10
    },        {
        label: "renameAllUpperCase",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">renameAllUpperCase</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@deprecated use DataFrame::renameEach() with a selected Style
                </div>
                            `
            return div
        },
        apply: snippet("renameAllUpperCase()"),
        boost: 10
    },        {
        label: "renameAllUpperCaseFirst",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">renameAllUpperCaseFirst</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@deprecated use DataFrame::renameEach() with a selected Style
                </div>
                            `
            return div
        },
        apply: snippet("renameAllUpperCaseFirst()"),
        boost: 10
    },        {
        label: "renameAllUpperCaseWord",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">renameAllUpperCaseWord</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@deprecated use DataFrame::renameEach() with a selected Style
                </div>
                            `
            return div
        },
        apply: snippet("renameAllUpperCaseWord()"),
        boost: 10
    },        {
        label: "renameEach",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">renameEach</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">RenameEntryStrategy</span> <span class=\"fn-param\">$strategies</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                            `
            return div
        },
        apply: snippet("renameEach(" + "$" + "{" + "1:strategies" + "}" + ")"),
        boost: 10
    },        {
        label: "reorderEntries",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">reorderEntries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Comparator</span> <span class=\"fn-param\">$comparator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">Flow\\ETL\\Transformer\\OrderEntries\\TypeComparator::...</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                            `
            return div
        },
        apply: snippet("reorderEntries(" + "$" + "{" + "1:comparator" + "}" + ")"),
        boost: 10
    },        {
        label: "rows",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">rows</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transformer|Transformation</span> <span class=\"fn-param\">$transformer</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>Alias for ETL::transform method.
                </div>
                            `
            return div
        },
        apply: snippet("rows(" + "$" + "{" + "1:transformer" + "}" + ")"),
        boost: 10
    },        {
        label: "run",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">run</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">callable</span> <span class=\"fn-param\">$callback</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">Analyze|bool</span> <span class=\"fn-param\">$analyze</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">false</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Report</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @trigger<br>When analyzing pipeline execution we can chose to collect various metrics through analyze()->with*() method<br>- column statistics - analyze()->withColumnStatistics()<br>- schema - analyze()->withSchema()<br>@param null|callable(Rows $rows, FlowContext $context): void $callback<br>@param Analyze|bool $analyze - when set run will return Report<br>@return ($analyze is Analyze|true ? Report : null)
                </div>
                            `
            return div
        },
        apply: snippet("run(" + "$" + "{" + "1:callback" + "}" + ", " + "$" + "{" + "2:analyze" + "}" + ")"),
        boost: 10
    },        {
        label: "saveMode",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">saveMode</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">SaveMode</span> <span class=\"fn-param\">$mode</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for DataFrame::mode.<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("saveMode(" + "$" + "{" + "1:mode" + "}" + ")"),
        boost: 10
    },        {
        label: "schema",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">schema</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">Schema</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @return Schema
                </div>
                            `
            return div
        },
        apply: snippet("schema()"),
        boost: 10
    },        {
        label: "select",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">select</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference|string</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>Keep only given entries.
                </div>
                            `
            return div
        },
        apply: snippet("select(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "sortBy",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">sortBy</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Reference</span> <span class=\"fn-param\">$entries</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("sortBy(" + "$" + "{" + "1:entries" + "}" + ")"),
        boost: 10
    },        {
        label: "transform",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">transform</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transformer|Transformation|Transformations|WithEntry</span> <span class=\"fn-param\">$transformer</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    Alias for DataFrame::with().<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("transform(" + "$" + "{" + "1:transformer" + "}" + ")"),
        boost: 10
    },        {
        label: "until",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">until</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">ScalarFunction</span> <span class=\"fn-param\">$function</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    The difference between filter and until is that filter will keep filtering rows until extractors finish yielding<br>rows. Until will send a STOP signal to the Extractor when the condition is not met.<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("until(" + "$" + "{" + "1:function" + "}" + ")"),
        boost: 10
    },        {
        label: "validate",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">validate</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Schema</span> <span class=\"fn-param\">$schema</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">SchemaValidator</span> <span class=\"fn-param\">$validator</span> <span class=\"fn-operator\">=</span> <span class=\"fn-default\">null</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @deprecated Please use DataFrame::match instead<br>@lazy<br>@param null|SchemaValidator $validator - when null, StrictValidator gets initialized
                </div>
                            `
            return div
        },
        apply: snippet("validate(" + "$" + "{" + "1:schema" + "}" + ", " + "$" + "{" + "2:validator" + "}" + ")"),
        boost: 10
    },        {
        label: "void",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">void</span><span class=\"fn-operator\">(</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>This method is useful mostly in development when<br>you want to pause processing at certain moment without<br>removing code. All operations will get processed up to this point,<br>from here no rows are passed forward.
                </div>
                            `
            return div
        },
        apply: snippet("void()"),
        boost: 10
    },        {
        label: "with",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">with</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Transformer|Transformation|Transformations|WithEntry</span> <span class=\"fn-param\">$transformer</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy
                </div>
                            `
            return div
        },
        apply: snippet("with(" + "$" + "{" + "1:transformer" + "}" + ")"),
        boost: 10
    },        {
        label: "withEntries",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">withEntries</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">array</span> <span class=\"fn-param\">$references</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>@param array<int, WithEntry>|array<string, ScalarFunction|WindowFunction|WithEntry> $references
                </div>
                            `
            return div
        },
        apply: snippet("withEntries(" + "$" + "{" + "1:references" + "}" + ")"),
        boost: 10
    },        {
        label: "withEntry",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">withEntry</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Definition|string</span> <span class=\"fn-param\">$entry</span><span class=\"fn-operator\">,</span> <span class=\"fn-type\">ScalarFunction|WindowFunction</span> <span class=\"fn-param\">$reference</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @param Definition<mixed>|string $entry<br>@lazy
                </div>
                            `
            return div
        },
        apply: snippet("withEntry(" + "$" + "{" + "1:entry" + "}" + ", " + "$" + "{" + "2:reference" + "}" + ")"),
        boost: 10
    },        {
        label: "write",
        type: "method",
        detail: "Flow\\\\ETL\\\\DataFrame",
        info: () => {
            const div = document.createElement("div")
            div.innerHTML = `
                <div style="font-family: 'Fira Code', 'JetBrains Mono', monospace; margin-bottom: 8px;">
                    <span class=\"fn-name\">write</span><span class=\"fn-operator\">(</span><span class=\"fn-type\">Loader</span> <span class=\"fn-param\">$loader</span><span class=\"fn-operator\">)</span> <span class=\"fn-operator\">:</span> <span class=\"fn-return\">self</span>
                </div>
                                <div style="color: #8b949e; font-size: 13px;">
                    @lazy<br>Alias for ETL::load function.
                </div>
                            `
            return div
        },
        apply: snippet("write(" + "$" + "{" + "1:loader" + "}" + ")"),
        boost: 10
    }    ]

/**
 * DataFrame method completion source for CodeMirror
 * @param {CompletionContext} context
 * @returns {CompletionResult|null}
 */
export function dataframeCompletions(context) {
    // Get text before cursor (potentially across multiple lines)
    // Look back up to 2000 characters to find the pattern
    const maxLookback = 2000
    const docText = context.state.doc.toString()
    const startPos = Math.max(0, context.pos - maxLookback)
    const textBefore = docText.slice(startPos, context.pos)

    // Check if we're directly after -> (method chaining context)
    // Match pattern: ->word* at the end
    if (!new RegExp('->\\w*$').test(textBefore)) {
        return null
    }

    // Collect all DataFrame-returning method names
    const allMethods = []
    for (const [className, methods] of Object.entries(dataframeReturningMethods)) {
        allMethods.push(...methods)
    }

    if (allMethods.length === 0) {
        return null
    }

    // Walk backwards to find the most recent completed method call at top level
    // Strategy: find the last occurrence of methodName()->... pattern where parens are balanced
    const methodPattern = new RegExp('\\b(' + allMethods.join('|') + ')\\s*\\(', 'g')
    let matches = []
    let match

    while ((match = methodPattern.exec(textBefore)) !== null) {
        matches.push({ name: match[1], index: match.index, endOfName: match.index + match[0].length })
    }

    // Walk backwards from cursor tracking parenthesis depth
    // to find what context we're in
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
            // If we're back to depth 0, check if this ( belongs to a DataFrame method
            if (depth === 0) {
                // Look backwards to find the method name
                let methodEnd = i
                while (methodEnd > 0 && /\s/.test(textBefore[methodEnd - 1])) {
                    methodEnd--
                }
                let methodStart = methodEnd
                while (methodStart > 0 && /\w/.test(textBefore[methodStart - 1])) {
                    methodStart--
                }
                const methodName = textBefore.slice(methodStart, methodEnd)

                // Check if this is a DataFrame-returning method
                if (allMethods.includes(methodName)) {
                    // This is it! We're directly after this method call
                    return continueWithCompletions()
                }
                // If not, we're done checking - we're inside some other call
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
        const options = dataframeMethods.filter(method =>
            !prefix || method.label.toLowerCase().startsWith(prefix)
        )

        return {
            from: word ? word.from : context.pos,
            options: options,
            validFor: new RegExp('^\\w*$')  // Reuse while typing word characters
        }
    }
}
