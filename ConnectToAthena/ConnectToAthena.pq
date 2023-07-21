[DataSource.Kind="AmazonAthena", Publish="AmazonAthena.Publish"]
shared AmazonAthena.Contents = (optional server as text, optional database as text) as table =>
    let
        server = Coalesce(server, "default"),
        database = Coalesce(database, "default"),
        url = Text.Format("https://athena.{0}.amazonaws.com", {server}),
        content = try Web.Contents(url,
            [
                Headers = [#"X-Amz-Target"="AmazonAthena.ListDatabases"],
                Content = Text.ToBinary("{""CatalogName"": """ & database & """}")
            ]
        ),
        result = if content[HasError] then
            error "Unable to connect to Amazon Athena"
        else
            let
                json = Json.Document(content[Value]),
                table = Table.FromRecords(json)
            in
                table
    in
        result;

// Data Source Kind description
AmazonAthena = [
    Authentication = [
        UsernamePassword = []
    ],
    Label = Extension.LoadString("DataSourceLabel")
];

// Data Source UI publishing description
AmazonAthena.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://docs.microsoft.com/en-us/power-query/connectors/amazonathena",
    SourceImage = AmazonAthena.Icons,
    SourceTypeImage = AmazonAthena.Icons
];

AmazonAthena.Icons = [
    Icon16 = { Extension.Contents("AmazonAthena16.png"), Extension.Contents("AmazonAthena20.png"), Extension.Contents("AmazonAthena24.png"), Extension.Contents("AmazonAthena32.png") },
    Icon32 = { Extension.Contents("AmazonAthena32.png"), Extension.Contents("AmazonAthena40.png"), Extension.Contents("AmazonAthena48.png"), Extension.Contents("AmazonAthena64.png") }
];
