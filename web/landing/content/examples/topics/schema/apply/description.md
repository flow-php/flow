While iterating through dataset that comes from a source which does not 
support strict schema, like CSV/XML/JSON, you can tell the extractor
what schema to apply to each read column.

Otherwise, DataFrame will try to guess the schema based on the data in the column.
It might be problematic if the first rows would be empty or null.
If the first row is a null, entry factory (mechanism responsible for creating entries)
will assume that the column is of type `string`.