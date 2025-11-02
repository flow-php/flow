export const flowCompleter = {
    getCompletions: function(editor, session, pos, prefix, callback) {
        const line = session.getLine(pos.row);
        const lineUpToCursor = line.substring(0, pos.column);

        if (shouldCompleteChainMethods(lineUpToCursor)) {
            callback(null, getScalarFunctionChainCompletions());
            return;
        }

        const completions = [];

        if (shouldCompleteRefFunction(lineUpToCursor, prefix)) {
            completions.push(createRefCompletion());
        }

        callback(null, completions);
    }
};

function shouldCompleteRefFunction(lineUpToCursor, prefix) {
    const refPattern = /\bref$/i;
    return refPattern.test(lineUpToCursor) || prefix === 'ref';
}

function shouldCompleteChainMethods(lineUpToCursor) {
    const chainPattern = /ref\s*\(\s*['"][^'"]*['"]\s*\)\s*->\s*\w*$/;
    return chainPattern.test(lineUpToCursor);
}

function createRefCompletion() {
    return {
        caption: 'ref',
        value: 'ref($0)',
        meta: 'Flow DSL',
        score: 1000,
        snippet: 'ref(${1:entry})'
    };
}

function getScalarFunctionChainCompletions() {
    const methods = [
        { name: 'lower', description: 'Convert string to lowercase', snippet: 'lower()' },
        { name: 'upper', description: 'Convert string to uppercase', snippet: 'upper()' },
        { name: 'trim', description: 'Remove whitespace from both sides', snippet: 'trim()' },
        { name: 'capitalize', description: 'Capitalize first letter', snippet: 'capitalize()' },

        { name: 'concat', description: 'Concatenate with another value', snippet: 'concat(${1:value})' },
        { name: 'append', description: 'Append value to string', snippet: 'append(${1:value})' },
        { name: 'prepend', description: 'Prepend value to string', snippet: 'prepend(${1:value})' },
        { name: 'contains', description: 'Check if string contains value', snippet: 'contains(${1:needle})' },
        { name: 'startsWith', description: 'Check if string starts with value', snippet: 'startsWith(${1:prefix})' },
        { name: 'endsWith', description: 'Check if string ends with value', snippet: 'endsWith(${1:suffix})' },
        { name: 'split', description: 'Split string by delimiter', snippet: 'split(${1:delimiter})' },
        { name: 'replace', description: 'Replace substring', snippet: 'replace(${1:search}, ${2:replace})' },

        { name: 'equals', description: 'Check if equal to value', snippet: 'equals(${1:value})' },
        { name: 'notEquals', description: 'Check if not equal to value', snippet: 'notEquals(${1:value})' },
        { name: 'same', description: 'Check if identical to value', snippet: 'same(${1:value})' },
        { name: 'notSame', description: 'Check if not identical to value', snippet: 'notSame(${1:value})' },
        { name: 'greaterThan', description: 'Check if greater than value', snippet: 'greaterThan(${1:value})' },
        { name: 'greaterThanEqual', description: 'Check if greater than or equal', snippet: 'greaterThanEqual(${1:value})' },
        { name: 'lessThan', description: 'Check if less than value', snippet: 'lessThan(${1:value})' },
        { name: 'lessThanEqual', description: 'Check if less than or equal', snippet: 'lessThanEqual(${1:value})' },
        { name: 'between', description: 'Check if value is between min and max', snippet: 'between(${1:min}, ${2:max})' },
        { name: 'isIn', description: 'Check if value is in array', snippet: 'isIn(${1:array})' },

        { name: 'isNull', description: 'Check if value is null', snippet: 'isNull()' },
        { name: 'isNotNull', description: 'Check if value is not null', snippet: 'isNotNull()' },
        { name: 'isTrue', description: 'Check if value is true', snippet: 'isTrue()' },
        { name: 'isFalse', description: 'Check if value is false', snippet: 'isFalse()' },
        { name: 'isEmpty', description: 'Check if value is empty', snippet: 'isEmpty()' },
        { name: 'isNumeric', description: 'Check if value is numeric', snippet: 'isNumeric()' },
        { name: 'isNotNumeric', description: 'Check if value is not numeric', snippet: 'isNotNumeric()' },
        { name: 'isEven', description: 'Check if number is even', snippet: 'isEven()' },
        { name: 'isOdd', description: 'Check if number is odd', snippet: 'isOdd()' },
        { name: 'isType', description: 'Check if value matches type', snippet: 'isType(${1:type})' },

        { name: 'arrayGet', description: 'Get value from array by key', snippet: 'arrayGet(${1:key})' },
        { name: 'arrayKeys', description: 'Get array keys', snippet: 'arrayKeys()' },
        { name: 'arrayValues', description: 'Get array values', snippet: 'arrayValues()' },
        { name: 'arrayFilter', description: 'Filter array elements', snippet: 'arrayFilter(${1:callback})' },
        { name: 'arrayMerge', description: 'Merge with another array', snippet: 'arrayMerge(${1:array})' },
        { name: 'arraySort', description: 'Sort array', snippet: 'arraySort()' },
        { name: 'arrayReverse', description: 'Reverse array', snippet: 'arrayReverse()' },
        { name: 'expand', description: 'Expand array into separate rows', snippet: 'expand()' },
        { name: 'unpack', description: 'Unpack array entries into columns', snippet: 'unpack()' },

        { name: 'plus', description: 'Add value', snippet: 'plus(${1:value})' },
        { name: 'minus', description: 'Subtract value', snippet: 'minus(${1:value})' },
        { name: 'multiply', description: 'Multiply by value', snippet: 'multiply(${1:value})' },
        { name: 'divide', description: 'Divide by value', snippet: 'divide(${1:value})' },
        { name: 'mod', description: 'Modulo operation', snippet: 'mod(${1:divisor})' },
        { name: 'power', description: 'Raise to power', snippet: 'power(${1:exponent})' },
        { name: 'round', description: 'Round number', snippet: 'round(${1:precision})' },

        { name: 'and', description: 'Logical AND', snippet: 'and(${1:condition})' },
        { name: 'andNot', description: 'Logical AND NOT', snippet: 'andNot(${1:condition})' },
        { name: 'or', description: 'Logical OR', snippet: 'or(${1:condition})' },
        { name: 'orNot', description: 'Logical OR NOT', snippet: 'orNot(${1:condition})' },

        { name: 'toDate', description: 'Convert to date', snippet: 'toDate()' },
        { name: 'toDateTime', description: 'Convert to datetime', snippet: 'toDateTime()' },
        { name: 'dateFormat', description: 'Format date', snippet: 'dateFormat(${1:format})' },

        { name: 'cast', description: 'Cast to type', snippet: 'cast(${1:type})' },
        { name: 'size', description: 'Get size of collection', snippet: 'size()' },
        { name: 'hash', description: 'Hash the value', snippet: 'hash(${1:algorithm})' },
        { name: 'jsonEncode', description: 'Encode as JSON', snippet: 'jsonEncode()' },
        { name: 'jsonDecode', description: 'Decode from JSON', snippet: 'jsonDecode()' },

        { name: 'as', description: 'Alias the reference', snippet: 'as(${1:alias})' },
        { name: 'desc', description: 'Sort descending', snippet: 'desc()' },
        { name: 'asc', description: 'Sort ascending', snippet: 'asc()' }
    ];

    return methods.map(method => ({
        caption: method.name,
        value: method.snippet,
        meta: method.description,
        score: 10000,
        snippet: method.snippet
    }));
}
