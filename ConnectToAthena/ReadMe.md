# Amazon Athena Power BI Connector

This repository contains the code for a Power BI custom connector for Amazon Athena. The connector is written in M language and allows users to connect to Amazon Athena from Power BI.

## Functionality

The main function in this connector is `AmazonAthena.Contents`. This function connects to Amazon Athena and returns a table. It accepts two optional parameters: `server` and `database`. If these parameters are not provided, the function defaults to using "default" for both.

```m
shared AmazonAthena.Contents = (optional server as text, optional database as text) as table => ...
```

## Error Handling

The connector uses the `try` keyword to handle any errors that might occur during the web request to Athena. If an error occurs, the function will return an error message. If the request is successful, the function will parse the JSON response from Athena and convert it into a table.

## Authentication

The `AmazonAthena` record in the connector defines the authentication method. In this case, the connector uses username and password authentication.

```m
AmazonAthena = [
    Authentication = [
        UsernamePassword = []
    ],
    Label = Extension.LoadString("DataSourceLabel")
];
```

## User Interface

The `AmazonAthena.Publish` record defines various UI elements for the connector, including the button text, help text, and icons. It also sets the connector to beta status and categorizes it as "Other".

```m
AmazonAthena.Publish = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://docs.microsoft.com/en-us/power-query/connectors/amazonathena",
    SourceImage = AmazonAthena.Icons,
    SourceTypeImage = AmazonAthena.Icons
];
```

## Icons

The `AmazonAthena.Icons` record defines the icons that will be used for the connector in the Power BI UI.

```m
AmazonAthena.Icons = [
    Icon16 = { Extension.Contents("AmazonAthena16.png"), Extension.Contents("AmazonAthena20.png"), Extension.Contents("AmazonAthena24.png"), Extension.Contents("AmazonAthena32.png") },
    Icon32 = { Extension.Contents("AmazonAthena32.png"), Extension.Contents("AmazonAthena40.png"), Extension.Contents("AmazonAthena48.png"), Extension.Contents("AmazonAthena64.png") }
];
```

Please note that the connector is currently in beta status. Feedback and contributions are welcome.
