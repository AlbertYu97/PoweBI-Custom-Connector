section AdvancedDataProcessing;

[DataSource.Kind = "AdvancedDataProcessing", Publish = "AdvancedDataProcessing.Publish"]
shared AdvancedDataProcessing.Contents = (optional message as text) =>
    let
        message = if (message <> null) then message else "Welcome to AdvancedDataProcessing",
        loadData = Extension.Contents("Data.csv"), // load data from a csv file
        transformData = Table.TransformColumnTypes(loadData,{{"ColumnName1", type text}, {"ColumnName2", type number}}), // change a column's type in the loaded data
        addedColumn = Table.AddColumn(transformData, "CalculatedColumn", each [ColumnName2] * 2), // add a new column that's twice the value of ColumnName2
        filteredData = Table.SelectRows(addedColumn, each [ColumnName1] <> "Invalid"), // filter out rows with invalid values in ColumnName1
        groupedData = Table.Group(filteredData, {"ColumnName1"}, {{"Count", each Table.RowCount(_), type number}}), // group by ColumnName1 and count rows
        sortedData = Table.Sort(groupedData, {{"Count", Order.Descending}}), // sort by Count in descending order
        outputData = sortedData
    in
        outputData;

AdvancedDataProcessing = [
    TestConnection = (dataSourcePath) => {"AdvancedDataProcessing.Contents", "AdvancedDataProcessing.Contents"},
    Authentication = [
        Anonymous = [],
        UsernamePassword = []
    ],
    Label = Extension.LoadString("DataSourceLabel")
];

AdvancedDataProcessing.Publish = [
    Beta = true,
    ButtonText = {Extension.LoadString("FormulaTitle"), Extension.LoadString("FormulaHelp")},
    SourceImage = AdvancedDataProcessing.Icons,
    SourceTypeImage = AdvancedDataProcessing.Icons
];

AdvancedDataProcessing.Icons = [
    Icon16 = {
        Extension.Contents("AdvancedDataProcessing16.png"),
        Extension.Contents("AdvancedDataProcessing20.png"),
        Extension.Contents("AdvancedDataProcessing24.png"),
        Extension.Contents("AdvancedDataProcessing32.png")
    },
    Icon32 = {
        Extension.Contents("AdvancedDataProcessing32.png"),
        Extension.Contents("AdvancedDataProcessing40.png"),
        Extension.Contents("AdvancedDataProcessing48.png"),
        Extension.Contents("AdvancedDataProcessing64.png")
    }
];

AdvancedDataProcessing.Test = [
    EntityFunctions = [
        Description = "Test AdvancedDataProcessing functions",
        TestFunction = (optional message as text) =>
            let
                result = AdvancedDataProcessing.Contents(message)
            in
                result
    ]
];
