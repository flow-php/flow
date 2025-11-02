ace.define("ace/theme/flow", ["require", "exports", "module", "ace/lib/dom"], function(require, exports, module) {
    exports.isDark = true;
    exports.cssClass = "ace-flow";
    exports.cssText = `
.ace-flow .ace_gutter {
    background: #1b1925;
    color: #8b949e;
}

.ace-flow .ace_print-margin {
    width: 1px;
    background: #30363d;
}

.ace-flow {
    background-color: #1b1925;
    color: #f8f8f2;
}

.ace-flow .ace_cursor {
    color: #f8f8f2;
}

.ace-flow .ace_marker-layer .ace_selection {
    background: rgba(121, 192, 255, 0.2);
}

.ace-flow.ace_multiselect .ace_selection.ace_start {
    box-shadow: 0 0 3px 0px #0d1117;
}

.ace-flow .ace_marker-layer .ace_step {
    background: rgb(102, 82, 0);
}

.ace-flow .ace_marker-layer .ace_bracket {
    margin: -1px 0 0 -1px;
    border: 1px solid rgba(255, 255, 255, 0.25);
}

.ace-flow .ace_marker-layer .ace_active-line {
    background: rgba(255, 255, 255, 0.05);
}

.ace-flow .ace_gutter-active-line {
    background-color: rgba(255, 255, 255, 0.05);
}

.ace-flow .ace_marker-layer .ace_selected-word {
    border: 1px solid rgba(121, 192, 255, 0.4);
}

.ace-flow .ace_invisible {
    color: rgba(255, 255, 255, 0.25);
}

.ace-flow .ace_keyword,
.ace-flow .ace_meta.ace_tag,
.ace-flow .ace_storage,
.ace-flow .ace_storage.ace_type,
.ace-flow .ace_support.ace_type,
.ace-flow .ace_meta.ace_php {
    color: #ff7b72;
}

.ace-flow .ace_keyword.ace_operator {
    color: #f8f8f2;
}

.ace-flow .ace_punctuation.ace_definition.ace_tag {
    color: #ff7b72;
}

.ace-flow .ace_constant.ace_character,
.ace-flow .ace_constant.ace_language,
.ace-flow .ace_constant.ace_numeric,
.ace-flow .ace_keyword.ace_other.ace_unit,
.ace-flow .ace_support.ace_constant,
.ace-flow .ace_variable.ace_parameter {
    color: #79c0ff;
}

.ace-flow .ace_constant.ace_other {
    color: #f8f8f2;
}

.ace-flow .ace_invalid {
    color: #f8f8f2;
    background-color: #ff7b72;
}

.ace-flow .ace_invalid.ace_deprecated {
    color: #f8f8f2;
    background-color: #d2a8ff;
}

.ace-flow .ace_fold {
    background-color: #ff7b72;
    border-color: #f8f8f2;
}

.ace-flow .ace_variable {
    color: #f8f8f2;
}

.ace-flow .ace_support.ace_class,
.ace-flow .ace_support.ace_type {
    color: #d2a8ff;
}

.ace-flow .ace_heading,
.ace-flow .ace_markup.ace_heading,
.ace-flow .ace_string {
    color: #79c0ff;
}

.ace-flow .ace_entity.ace_name.ace_tag,
.ace-flow .ace_entity.ace_other.ace_attribute-name,
.ace-flow .ace_meta.ace_tag,
.ace-flow .ace_string.ace_regexp {
    color: #79c0ff;
}

.ace-flow .ace_comment {
    color: #8292a2;
}

.ace-flow .ace_entity.ace_name.ace_function,
.ace-flow .ace_support.ace_function,
.ace-flow .ace_meta.ace_function-call,
.ace-flow .ace_meta.ace_function-call .ace_entity.ace_name.ace_function,
.ace-flow .ace_entity.ace_name.ace_function-call,
.ace-flow .ace_identifier {
    color: #d2a8ff !important;
}

.ace-flow .ace_indent-guide {
    background: url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAACCAYAAACZgbYnAAAAEklEQVQImWNgYGBgYHB3d/8PAAOIAdULw8qMAAAAAElFTkSuQmCC) right repeat-y;
}

.ace-flow .ace_indent-guide-active {
    background: url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAACCAYAAACZgbYnAAAAEklEQVQIW2NgYGBgYHjy5Ml/ACIKAsyJx1gMAAAAAElFTkSuQmCC) right repeat-y;
}

/* === ERROR LINE HIGHLIGHTING === */
.ace-flow .ace_error-line {
    position: absolute;
    background-color: rgba(255, 123, 114, 0.15) !important;
    border-left: 3px solid #ff7b72 !important;
}

/* === AUTOCOMPLETE STYLING === */
.ace_autocomplete {
    background: #21212b !important;
    border: 2px solid #79c0ff !important;
    border-radius: 8px !important;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5) !important;
    width: 550px !important;
    font-family: "Cabin Variable", system-ui !important;
    font-size: 16px !important;
    line-height: 1.6 !important;
    padding: 6px !important;
}

.ace_autocomplete .ace_line {
    padding: 8px 14px !important;
    border-radius: 5px !important;
    margin: 2px 0 !important;
    color: #f8f8f2 !important;
}

.ace_autocomplete .ace_line.ace_selected {
    background: linear-gradient(90deg, #79c0ff 0%, #6bb0f0 100%) !important;
    color: #0d1117 !important;
    font-weight: 600 !important;
    box-shadow: 0 2px 8px rgba(121, 192, 255, 0.3) !important;
}

.ace_autocomplete .ace_completion-highlight {
    color: #ff7b72 !important;
    font-weight: 700 !important;
}

.ace_autocomplete .ace_line.ace_selected .ace_completion-highlight {
    color: #0d1117 !important;
}

.ace_autocomplete .ace_rightAlignedText,
.ace_autocomplete .ace_completion-meta {
    color: #8b949e !important;
    font-size: 12px !important;
    font-style: italic !important;
    margin-left: 16px !important;
}

.ace_autocomplete .ace_line.ace_selected .ace_completion-meta {
    color: #0d1117 !important;
    font-weight: 500 !important;
}

.ace_doc-tooltip {
    background: #21212b !important;
    border: 2px solid #79c0ff !important;
    border-radius: 8px !important;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5) !important;
    color: #f8f8f2 !important;
    font-family: "Cabin Variable", system-ui !important;
    font-size: 13px !important;
    max-width: 500px !important;
    padding: 0 !important;
}

.ace_doc-tooltip code {
    background: #1b1925 !important;
    padding: 4px 8px !important;
    border-radius: 4px !important;
    color: #ff7b72 !important;
    font-family: monospace !important;
}
`;

    var dom = require("../lib/dom");
    dom.importCssString(exports.cssText, exports.cssClass, false);
});
(function() {
    ace.require(["ace/theme/flow"], function(m) {
        if (typeof module == "object" && typeof exports == "object" && module) {
            module.exports = m;
        }
    });
})();
