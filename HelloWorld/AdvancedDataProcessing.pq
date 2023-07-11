section AdvancedDataProcessing;

[DataSource.Kind = "AdvancedDataProcessing", Publish = "AdvancedDataProcessing.Publish"]
shared AdvancedDataProcessing.Contents = (optional message as text) =>
    let
        message = if (message <> null) then message else "Welcome to AdvancedDataProcessing",
        loadData = Extension.Contents("Data.csv"), // load data from a csv file
        transformData = Table.TransformColumnTypes(loadData,{{"ColumnName", type text}}), // change a column's type in the loaded data
        outputData = Table.SelectRows(transformData, each [ColumnName] <> "Invalid") // filter out rows with invalid values in a particular column
    in
        outputData;

AdvancedDataProcessing = [
    TestConnection = (dataSourcePath) => {"AdvancedDataProcessing.Contents"},
    Authentication = [
        Anonymous = []
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
