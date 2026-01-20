// Authorization
"v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

// Tags
let
    auth  = Text.From(Authorization),

    // Reduce baseUrl to scheme://host[:port] so we never leak /comGpsGate/... to the host part
     BaseHost = "https://omantracking2.com/",
    appId = Text.From(applicationId),

    // Build x-www-form-urlencoded payload cleanly
    FormBody = Uri.BuildQueryString([
      method       = "GET",
      token        = auth,
      base_url     = BaseHost,
      path    = "comGpsGate/api/v.1/applications/" & appId & "/tags"
    ]),

    // Static host; variable bits are in Content only
    Response =
      Json.Document(
        Web.Contents(
          "https://powerbixgpsgatexgdriver.onrender.com",
          [
            RelativePath = "api",
            Content      = Text.ToBinary(FormBody),
            Headers      = [ #"Content-Type" = "application/x-www-form-urlencoded" ]
          ]
        )
      ),
    // Static host; variable bits are in Content only
    data = Response[data],
    #"Converted to Table" = Table.FromList(data, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"applicationId", "description", "id", "name", "usersIds"}, {"Column1.applicationId", "Column1.description", "Column1.id", "Column1.name", "Column1.usersIds"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded Column1",{"Column1.id", "Column1.name"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Other Columns",{{"Column1.id", "id"}, {"Column1.name", "name"}}),
    #"Filtered Rows" = Table.SelectRows(#"Renamed Columns", each ([name] = "Show on map"))
in
    #"Filtered Rows"

// applicationId
"6" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

// EventRules
let
    auth  = Text.From(Authorization),

    // Reduce baseUrl to scheme://host[:port] so we never leak /comGpsGate/... to the host part
     BaseHost = "https://omantracking2.com/",
    appId = Text.From(applicationId),

    // Build x-www-form-urlencoded payload cleanly
    FormBody = Uri.BuildQueryString([
      method       = "GET",
      token        = auth,
      base_url     = BaseHost,
      path    = "comGpsGate/api/v.1/applications/" & appId & "/eventrules"
    ]),

    // Static host; variable bits are in Content only
    Response =
      Json.Document(
        Web.Contents(
          "https://powerbixgpsgatexgdriver.onrender.com",
          [
            RelativePath = "api",
            Content      = Text.ToBinary(FormBody),
            Headers      = [ #"Content-Type" = "application/x-www-form-urlencoded" ]
          ]
        )
      ),
    // Static host; variable bits are in Content only
    data = Response[data],
    #"Converted to Table" = Table.FromList(data, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"applicationId", "description", "id", "name", "usersIds"}, {"Column1.applicationId", "Column1.description", "Column1.id", "Column1.name", "Column1.usersIds"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded Column1",{"Column1.id", "Column1.name"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Other Columns",{{"Column1.id", "id"}, {"Column1.name", "name"}})
in
    #"Renamed Columns"

// Reports
let
    auth  = Text.From(Authorization),

    // Reduce baseUrl to scheme://host[:port] so we never leak /comGpsGate/... to the host part
     BaseHost = "https://omantracking2.com/",
    appId = Text.From(applicationId),

    // Build x-www-form-urlencoded payload cleanly
    FormBody = Uri.BuildQueryString([
      method       = "GET",
      token        = auth,
      base_url     = BaseHost,
      path    = "comGpsGate/api/v.1/applications/" & appId & "/reports"
    ]),

    // Static host; variable bits are in Content only
    Response =
      Json.Document(
        Web.Contents(
          "https://powerbixgpsgatexgdriver.onrender.com",
          [
            RelativePath = "api",
            Content      = Text.ToBinary(FormBody),
            Headers      = [ #"Content-Type" = "application/x-www-form-urlencoded" ]
          ]
        )
      ),
    // Static host; variable bits are in Content only
    data = Response[data],
    #"Converted to Table" = Table.FromList(data, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"applicationId", "description", "id", "name", "usersIds"}, {"Column1.applicationId", "Column1.description", "Column1.id", "Column1.name", "Column1.usersIds"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded Column1",{"Column1.id", "Column1.name"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Other Columns",{{"Column1.id", "id"}, {"Column1.name", "name"}})
in
    #"Renamed Columns"

// Vehicles
let
    auth  = Text.From(Authorization),

    // Reduce baseUrl to scheme://host[:port] so we never leak /comGpsGate/... to the host part
     BaseHost = "https://omantracking2.com/",
    appId = Text.From(applicationId),

    // Build x-www-form-urlencoded payload cleanly
    FormBody = Uri.BuildQueryString([
      method       = "GET",
      token        = auth,
      base_url     = BaseHost,
      path    = "comGpsGate/api/v.1/applications/" & appId & "/users"
    ]),

    // Static host; variable bits are in Content only
    Response =
      Json.Document(
        Web.Contents(
          "https://powerbixgpsgatexgdriver.onrender.com",
          [
            RelativePath = "api",
            Content      = Text.ToBinary(FormBody),
            Headers      = [ #"Content-Type" = "application/x-www-form-urlencoded" ]
          ]
        )
      ),
    // Static host; variable bits are in Content only
    data = Response[data],
    #"Converted to Table" = Table.FromList(data, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"calculatedSpeed", "deviceActivity", "devices", "email", "id", "name", "originalApplicationID", "surname", "trackPoint", "userTemplateID", "username", "lastTransport", "description", "driverID"}, {"Column1.calculatedSpeed", "Column1.deviceActivity", "Column1.devices", "Column1.email", "Column1.id", "Column1.name", "Column1.originalApplicationID", "Column1.surname", "Column1.trackPoint", "Column1.userTemplateID", "Column1.username", "Column1.lastTransport", "Column1.description", "Column1.driverID"}),
    #"Expanded Column1.trackPoint" = Table.ExpandRecordColumn(#"Expanded Column1", "Column1.trackPoint", {"position", "utc", "valid", "velocity"}, {"Column1.trackPoint.position", "Column1.trackPoint.utc", "Column1.trackPoint.valid", "Column1.trackPoint.velocity"}),
    #"Expanded Column1.trackPoint.velocity" = Table.ExpandRecordColumn(#"Expanded Column1.trackPoint", "Column1.trackPoint.velocity", {"groundSpeed", "heading"}, {"Column1.trackPoint.velocity.groundSpeed", "Column1.trackPoint.velocity.heading"}),
    #"Expanded Column1.trackPoint.position" = Table.ExpandRecordColumn(#"Expanded Column1.trackPoint.velocity", "Column1.trackPoint.position", {"altitude", "latitude", "longitude"}, {"Column1.trackPoint.position.altitude", "Column1.trackPoint.position.latitude", "Column1.trackPoint.position.longitude"}),
    #"Expanded Column1.devices" = Table.ExpandListColumn(#"Expanded Column1.trackPoint.position", "Column1.devices"),
    #"Expanded Column1.devices1" = Table.ExpandRecordColumn(#"Expanded Column1.devices", "Column1.devices", {"apn", "created", "deviceDefinitionID", "devicePassword", "gprsPassword", "gprsUsername", "hidePosition", "id", "imei", "lastIP", "lastPort", "latitude", "longitude", "mobileNetworkID", "msgFieldDictionaryID", "msisdn", "name", "oneWireVariables", "ownerEmail", "ownerID", "ownerName", "ownerUsername", "profileId", "protocolID", "protocolVersionID", "proximity", "staticIP", "staticPort", "timeStamp", "email"}, {"Column1.devices.apn", "Column1.devices.created", "Column1.devices.deviceDefinitionID", "Column1.devices.devicePassword", "Column1.devices.gprsPassword", "Column1.devices.gprsUsername", "Column1.devices.hidePosition", "Column1.devices.id", "Column1.devices.imei", "Column1.devices.lastIP", "Column1.devices.lastPort", "Column1.devices.latitude", "Column1.devices.longitude", "Column1.devices.mobileNetworkID", "Column1.devices.msgFieldDictionaryID", "Column1.devices.msisdn", "Column1.devices.name", "Column1.devices.oneWireVariables", "Column1.devices.ownerEmail", "Column1.devices.ownerID", "Column1.devices.ownerName", "Column1.devices.ownerUsername", "Column1.devices.profileId", "Column1.devices.protocolID", "Column1.devices.protocolVersionID", "Column1.devices.proximity", "Column1.devices.staticIP", "Column1.devices.staticPort", "Column1.devices.timeStamp", "Column1.devices.email"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded Column1.devices1",{"Column1.trackPoint.position.longitude","Column1.trackPoint.position.latitude",
         "Column1.trackPoint.utc","Column1.trackPoint.valid","Column1.username","Column1.name",
         "Column1.devices.name","Column1.devices.imei","Column1.id"}),
    #"Removed Errors" = Table.RenameColumns(#"Removed Other Columns",
        {{"Column1.trackPoint.position.longitude","Longitude"},
         {"Column1.trackPoint.position.latitude","Latitude"},
         {"Column1.trackPoint.utc","utc"},
         {"Column1.trackPoint.valid","validity"},
         {"Column1.username","username"},
         {"Column1.name","name"},
         {"Column1.devices.name","device"},
         {"Column1.devices.imei","imei"},
         {"Column1.id","id"}}),
    #"Removed Columns" = Table.Distinct(#"Removed Errors", {"name"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Columns", each ([imei] <> null)),
    #"Appended Query" = Table.Combine({#"Filtered Rows", #"Vehicles (5)"}),
    #"Removed Duplicates" = Table.Distinct(#"Appended Query", {"name"})
in
    #"Removed Duplicates"

// Vehicles (5)
// fnUsers_Direct_OnlyFromIndex

  let
  baseUrl = "https://omantracking2.com",
    Base =
      if Text.EndsWith(baseUrl, "/")
      then Text.Start(baseUrl, Text.Length(baseUrl)-1)
      else baseUrl,
    Response =
      Json.Document(
        Web.Contents(
          Base,
          [
            RelativePath = "comGpsGate/api/v.1/applications/" & Text.From(applicationId) & "/users",
            Query        = [ FromIndex = Text.From("17508") ],
            Headers      = [ Authorization = Authorization ]
          ])),
    UsersList =
      if Value.Is(Response, type list) then Response
      else if Value.Is(Response, type record) and Record.HasFields(Response, "users") then Response[users]
      else error "Unexpected response shape.",
    T = Table.FromList(UsersList, Splitter.SplitByNothing(), {"Record"}),
    AllCols  = List.Union(List.Transform(T[Record], each Record.FieldNames(_))),
    Expanded = Table.ExpandRecordColumn(T, "Record", AllCols, AllCols),
    #"Expanded trackPoint" = Table.ExpandRecordColumn(Expanded, "trackPoint", {"position", "velocity", "utc", "valid"}, {"trackPoint.position", "trackPoint.velocity", "trackPoint.utc", "trackPoint.valid"}),
    #"Expanded trackPoint.position" = Table.ExpandRecordColumn(#"Expanded trackPoint", "trackPoint.position", {"altitude", "longitude", "latitude"}, {"trackPoint.position.altitude", "trackPoint.position.longitude", "trackPoint.position.latitude"}),
    #"Expanded trackPoint.velocity" = Table.ExpandRecordColumn(#"Expanded trackPoint.position", "trackPoint.velocity", {"groundSpeed", "heading"}, {"trackPoint.velocity.groundSpeed", "trackPoint.velocity.heading"}),
    #"Expanded devices" = Table.ExpandListColumn(#"Expanded trackPoint.velocity", "devices"),
    #"Expanded devices1" = Table.ExpandRecordColumn(#"Expanded devices", "devices", {"id", "created", "name", "hidePosition", "proximity", "imei", "msisdn", "email", "apn", "gprsUsername", "gprsPassword", "lastIP", "lastPort", "staticIP", "staticPort", "protocolID", "profileId", "protocolVersionID", "msgFieldDictionaryID", "deviceDefinitionID", "mobileNetworkID", "longitude", "latitude", "timeStamp", "ownerID", "ownerUsername", "ownerName", "ownerEmail", "devicePassword", "oneWireVariables"}, {"devices.id", "devices.created", "devices.name", "devices.hidePosition", "devices.proximity", "devices.imei", "devices.msisdn", "devices.email", "devices.apn", "devices.gprsUsername", "devices.gprsPassword", "devices.lastIP", "devices.lastPort", "devices.staticIP", "devices.staticPort", "devices.protocolID", "devices.profileId", "devices.protocolVersionID", "devices.msgFieldDictionaryID", "devices.deviceDefinitionID", "devices.mobileNetworkID", "devices.longitude", "devices.latitude", "devices.timeStamp", "devices.ownerID", "devices.ownerUsername", "devices.ownerName", "devices.ownerEmail", "devices.devicePassword", "devices.oneWireVariables"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded devices1",{"trackPoint.position.longitude","trackPoint.position.latitude",
         "trackPoint.utc","trackPoint.valid","username","name",
         "devices.name","devices.imei","id"}),
    #"Removed Errors" = Table.RenameColumns(#"Removed Other Columns",
        {{"trackPoint.position.longitude","Longitude"},
         {"trackPoint.position.latitude","Latitude"},
         {"trackPoint.utc","utc"},
         {"trackPoint.valid","validity"},
         {"username","username"},
         {"name","name"},
         {"devices.name","device"},
         {"devices.imei","imei"},
         {"id","id"}}),
    #"Removed Columns" = Table.Distinct(#"Removed Errors", {"name"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Columns", each ([imei] <> null))
  in
    #"Filtered Rows"

// Drivers
let
    auth  = Text.From(Authorization),

    // Reduce baseUrl to scheme://host[:port] so we never leak /comGpsGate/... to the host part
     BaseHost = "https://omantracking2.com/",
    appId = Text.From(applicationId),

    // Build x-www-form-urlencoded payload cleanly
    FormBody = Uri.BuildQueryString([
      method       = "GET",
      token        = auth,
      base_url     = BaseHost,
      path    = "comGpsGate/api/v.1/applications/" & appId & "/users"
    ]),

    // Static host; variable bits are in Content only
    Response =
      Json.Document(
        Web.Contents(
          "https://powerbixgpsgatexgdriver.onrender.com",
          [
            RelativePath = "api",
            Content      = Text.ToBinary(FormBody),
            Headers      = [ #"Content-Type" = "application/x-www-form-urlencoded" ]
          ]
        )
      ),
    // Static host; variable bits are in Content only
    data = Response[data],
    #"Converted to Table" = Table.FromList(data, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"calculatedSpeed", "deviceActivity", "devices", "email", "id", "name", "originalApplicationID", "surname", "trackPoint", "userTemplateID", "username", "lastTransport", "description", "driverID"}, {"Column1.calculatedSpeed", "Column1.deviceActivity", "Column1.devices", "Column1.email", "Column1.id", "Column1.name", "Column1.originalApplicationID", "Column1.surname", "Column1.trackPoint", "Column1.userTemplateID", "Column1.username", "Column1.lastTransport", "Column1.description", "Column1.driverID"}),
    #"Expanded Column1.trackPoint" = Table.ExpandRecordColumn(#"Expanded Column1", "Column1.trackPoint", {"position", "utc", "valid", "velocity"}, {"Column1.trackPoint.position", "Column1.trackPoint.utc", "Column1.trackPoint.valid", "Column1.trackPoint.velocity"}),
    #"Expanded Column1.trackPoint.velocity" = Table.ExpandRecordColumn(#"Expanded Column1.trackPoint", "Column1.trackPoint.velocity", {"groundSpeed", "heading"}, {"Column1.trackPoint.velocity.groundSpeed", "Column1.trackPoint.velocity.heading"}),
    #"Expanded Column1.trackPoint.position" = Table.ExpandRecordColumn(#"Expanded Column1.trackPoint.velocity", "Column1.trackPoint.position", {"altitude", "latitude", "longitude"}, {"Column1.trackPoint.position.altitude", "Column1.trackPoint.position.latitude", "Column1.trackPoint.position.longitude"}),
    #"Expanded Column1.devices" = Table.ExpandListColumn(#"Expanded Column1.trackPoint.position", "Column1.devices"),
    #"Expanded Column1.devices1" = Table.ExpandRecordColumn(#"Expanded Column1.devices", "Column1.devices", {"apn", "created", "deviceDefinitionID", "devicePassword", "gprsPassword", "gprsUsername", "hidePosition", "id", "imei", "lastIP", "lastPort", "latitude", "longitude", "mobileNetworkID", "msgFieldDictionaryID", "msisdn", "name", "oneWireVariables", "ownerEmail", "ownerID", "ownerName", "ownerUsername", "profileId", "protocolID", "protocolVersionID", "proximity", "staticIP", "staticPort", "timeStamp", "email"}, {"Column1.devices.apn", "Column1.devices.created", "Column1.devices.deviceDefinitionID", "Column1.devices.devicePassword", "Column1.devices.gprsPassword", "Column1.devices.gprsUsername", "Column1.devices.hidePosition", "Column1.devices.id", "Column1.devices.imei", "Column1.devices.lastIP", "Column1.devices.lastPort", "Column1.devices.latitude", "Column1.devices.longitude", "Column1.devices.mobileNetworkID", "Column1.devices.msgFieldDictionaryID", "Column1.devices.msisdn", "Column1.devices.name", "Column1.devices.oneWireVariables", "Column1.devices.ownerEmail", "Column1.devices.ownerID", "Column1.devices.ownerName", "Column1.devices.ownerUsername", "Column1.devices.profileId", "Column1.devices.protocolID", "Column1.devices.protocolVersionID", "Column1.devices.proximity", "Column1.devices.staticIP", "Column1.devices.staticPort", "Column1.devices.timeStamp", "Column1.devices.email"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded Column1.devices1",{"Column1.trackPoint.position.longitude","Column1.trackPoint.position.latitude",
         "Column1.trackPoint.utc","Column1.trackPoint.valid","Column1.username","Column1.name",
         "Column1.devices.name","Column1.devices.imei","Column1.id","Column1.driverID"}),
    #"Removed Errors" = Table.RenameColumns(#"Removed Other Columns",
        {{"Column1.trackPoint.position.longitude","Longitude"},
         {"Column1.trackPoint.position.latitude","Latitude"},
         {"Column1.trackPoint.utc","utc"},
         {"Column1.trackPoint.valid","validity"},
         {"Column1.username","username"},
         {"Column1.name","name"},
         {"Column1.devices.name","device"},
         {"Column1.devices.imei","imei"},
         {"Column1.driverID","driverID"},
         {"Column1.id","id"}}),
    #"Removed Columns" = Table.Distinct(#"Removed Errors", {"name"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Columns", each ([driverID] <> null)),
    #"Appended Query" = Table.Combine({#"Filtered Rows", #"Drivers (2)"}),
    #"Removed Duplicates" = Table.Distinct(#"Appended Query", {"name"})
in
    #"Removed Duplicates"

// Drivers (2)
// fnUsers_Direct_OnlyFromIndex

  let
  baseUrl = "https://omantracking2.com",
    Base =
      if Text.EndsWith(baseUrl, "/")
      then Text.Start(baseUrl, Text.Length(baseUrl)-1)
      else baseUrl,
    Response =
      Json.Document(
        Web.Contents(
          Base,
          [
            RelativePath = "comGpsGate/api/v.1/applications/" & Text.From(applicationId) & "/users",
            Query        = [ FromIndex = Text.From("14790") ],
            Headers      = [ Authorization = Authorization ]
          ])),
    UsersList =
      if Value.Is(Response, type list) then Response
      else if Value.Is(Response, type record) and Record.HasFields(Response, "users") then Response[users]
      else error "Unexpected response shape.",
    T = Table.FromList(UsersList, Splitter.SplitByNothing(), {"Record"}),
    AllCols  = List.Union(List.Transform(T[Record], each Record.FieldNames(_))),
    Expanded = Table.ExpandRecordColumn(T, "Record", AllCols, AllCols),
    #"Expanded trackPoint" = Table.ExpandRecordColumn(Expanded, "trackPoint", {"position", "velocity", "utc", "valid"}, {"trackPoint.position", "trackPoint.velocity", "trackPoint.utc", "trackPoint.valid"}),
    #"Expanded trackPoint.position" = Table.ExpandRecordColumn(#"Expanded trackPoint", "trackPoint.position", {"altitude", "longitude", "latitude"}, {"trackPoint.position.altitude", "trackPoint.position.longitude", "trackPoint.position.latitude"}),
    #"Expanded trackPoint.velocity" = Table.ExpandRecordColumn(#"Expanded trackPoint.position", "trackPoint.velocity", {"groundSpeed", "heading"}, {"trackPoint.velocity.groundSpeed", "trackPoint.velocity.heading"}),
    #"Expanded devices" = Table.ExpandListColumn(#"Expanded trackPoint.velocity", "devices"),
    #"Expanded devices1" = Table.ExpandRecordColumn(#"Expanded devices", "devices", {"id", "created", "name", "hidePosition", "proximity", "imei", "msisdn", "email", "apn", "gprsUsername", "gprsPassword", "lastIP", "lastPort", "staticIP", "staticPort", "protocolID", "profileId", "protocolVersionID", "msgFieldDictionaryID", "deviceDefinitionID", "mobileNetworkID", "longitude", "latitude", "timeStamp", "ownerID", "ownerUsername", "ownerName", "ownerEmail", "devicePassword", "oneWireVariables"}, {"devices.id", "devices.created", "devices.name", "devices.hidePosition", "devices.proximity", "devices.imei", "devices.msisdn", "devices.email", "devices.apn", "devices.gprsUsername", "devices.gprsPassword", "devices.lastIP", "devices.lastPort", "devices.staticIP", "devices.staticPort", "devices.protocolID", "devices.profileId", "devices.protocolVersionID", "devices.msgFieldDictionaryID", "devices.deviceDefinitionID", "devices.mobileNetworkID", "devices.longitude", "devices.latitude", "devices.timeStamp", "devices.ownerID", "devices.ownerUsername", "devices.ownerName", "devices.ownerEmail", "devices.devicePassword", "devices.oneWireVariables"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded devices1",{"trackPoint.position.longitude","trackPoint.position.latitude",
         "trackPoint.utc","trackPoint.valid","username","name",
         "devices.name","devices.imei","id","driverID"}),
    #"Removed Errors" = Table.RenameColumns(#"Removed Other Columns",
        {{"trackPoint.position.longitude","Longitude"},
         {"trackPoint.position.latitude","Latitude"},
         {"trackPoint.utc","utc"},
         {"trackPoint.valid","validity"},
         {"username","username"},
         {"name","name"},
         {"devices.name","device"},
         {"devices.imei","imei"},
         {"id","id"}}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Errors", each ([driverID] <> null))
in
    #"Filtered Rows"

// Vehicles Department
let
    // Params: baseUrl (text), applicationId (number/text), Authorization (text)

    // Normalize base to scheme://host[:port]
    Base = Vehicles,

    #"Removed Duplicates"  = Table.Distinct(Base, {"name"}),
    #"Removed Other Columns2" = Table.SelectColumns(#"Removed Duplicates", {"id"}),
    // Make sure fnRequestCustomField is implemented with Web.Contents(Base,[RelativePath=...,Headers=...])
    #"Added Custom Fields" = Table.AddColumn(#"Removed Other Columns2", "CustomFields", each fnRequestCustomField("https://omantracking2.com",applicationId,Text.From([id]))),
    #"Expanded CustomFields"  = Table.ExpandListColumn(#"Added Custom Fields", "CustomFields"),
    #"Expanded CustomFields2" = Table.ExpandRecordColumn(#"Expanded CustomFields", "CustomFields", {"Installation Location", "Installation Date", "Vehicle Brand", "Department"}, {"CustomFields.Installation Location", "CustomFields.Installation Date", "CustomFields.Vehicle Brand", "CustomFields.Department"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded CustomFields2",{"id", "CustomFields.Department"})
in
    #"Removed Other Columns"

// fnRequestCustomField
// fnUserCustomFields_Direct
// Usage: fnUserCustomFields_Direct("https://omantracking2.com/", 123, 456, Authorization)
let
  fnUserCustomFields_Direct =
  (baseUrl as text, applicationId as any, userId as any) as table =>
  let
    Source = Json.Document(
        Web.Contents(
            "https://omantracking2.com/",
            [
                RelativePath = "comGpsGate/api/v.1/applications/" & applicationId & "/users/" & userId & "/customfields",
                Headers = [
                    Authorization = Authorization,
                    #"Content-Type" = "application/json"
                ]
            ]
        )
    ),
    #"Converted to Table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"name", "value"}, {"Column1.name", "Column1.value"}),
    #"Pivoted Column" = Table.Pivot(#"Expanded Column1", List.Distinct(#"Expanded Column1"[Column1.name]), "Column1.name", "Column1.value")
in
    #"Pivoted Column"  

in
  fnUserCustomFields_Direct

// Vehicle Brand
let
    // Params: baseUrl (text), applicationId (number/text), Authorization (text)

    // Normalize base to scheme://host[:port]
    Base = Vehicles,

    #"Removed Duplicates"  = Table.Distinct(Base, {"name"}),
    #"Removed Other Columns2" = Table.SelectColumns(#"Removed Duplicates", {"id"}),
    // Make sure fnRequestCustomField is implemented with Web.Contents(Base,[RelativePath=...,Headers=...])
    #"Added Custom Fields" = Table.AddColumn(#"Removed Other Columns2", "CustomFields", each fnRequestCustomField("https://omantracking2.com",applicationId,Text.From([id]))),
    #"Expanded CustomFields"  = Table.ExpandListColumn(#"Added Custom Fields", "CustomFields"),
    #"Expanded CustomFields2" = Table.ExpandRecordColumn(#"Expanded CustomFields", "CustomFields", {"Installation Location", "Installation Date", "Vehicle Brand", "Department"}, {"CustomFields.Installation Location", "CustomFields.Installation Date", "CustomFields.Vehicle Brand", "CustomFields.Department"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded CustomFields2",{"id", "CustomFields.Vehicle Brand"})
in
    #"Removed Other Columns"

// Vehicles (6)
// fnUsers_Direct_OnlyFromIndex

  let
  baseUrl = "https://omantracking2.com",
    Base =
      if Text.EndsWith(baseUrl, "/")
      then Text.Start(baseUrl, Text.Length(baseUrl)-1)
      else baseUrl,
    Response =
      Json.Document(
        Web.Contents(
          Base,
          [
            RelativePath = "comGpsGate/api/v.1/applications/" & Text.From(applicationId) & "/users",
            Query        = [ FromIndex = Text.From("29063") ],
            Headers      = [ Authorization = Authorization ]
          ])),
    UsersList =
      if Value.Is(Response, type list) then Response
      else if Value.Is(Response, type record) and Record.HasFields(Response, "users") then Response[users]
      else error "Unexpected response shape.",
    T = Table.FromList(UsersList, Splitter.SplitByNothing(), {"Record"}),
    AllCols  = List.Union(List.Transform(T[Record], each Record.FieldNames(_))),
    Expanded = Table.ExpandRecordColumn(T, "Record", AllCols, AllCols),
    #"Expanded trackPoint" = Table.ExpandRecordColumn(Expanded, "trackPoint", {"position", "velocity", "utc", "valid"}, {"trackPoint.position", "trackPoint.velocity", "trackPoint.utc", "trackPoint.valid"}),
    #"Expanded trackPoint.position" = Table.ExpandRecordColumn(#"Expanded trackPoint", "trackPoint.position", {"altitude", "longitude", "latitude"}, {"trackPoint.position.altitude", "trackPoint.position.longitude", "trackPoint.position.latitude"}),
    #"Expanded trackPoint.velocity" = Table.ExpandRecordColumn(#"Expanded trackPoint.position", "trackPoint.velocity", {"groundSpeed", "heading"}, {"trackPoint.velocity.groundSpeed", "trackPoint.velocity.heading"}),
    #"Expanded devices" = Table.ExpandListColumn(#"Expanded trackPoint.velocity", "devices"),
    #"Expanded devices1" = Table.ExpandRecordColumn(#"Expanded devices", "devices", {"id", "created", "name", "hidePosition", "proximity", "imei", "msisdn", "email", "apn", "gprsUsername", "gprsPassword", "lastIP", "lastPort", "staticIP", "staticPort", "protocolID", "profileId", "protocolVersionID", "msgFieldDictionaryID", "deviceDefinitionID", "mobileNetworkID", "longitude", "latitude", "timeStamp", "ownerID", "ownerUsername", "ownerName", "ownerEmail", "devicePassword", "oneWireVariables"}, {"devices.id", "devices.created", "devices.name", "devices.hidePosition", "devices.proximity", "devices.imei", "devices.msisdn", "devices.email", "devices.apn", "devices.gprsUsername", "devices.gprsPassword", "devices.lastIP", "devices.lastPort", "devices.staticIP", "devices.staticPort", "devices.protocolID", "devices.profileId", "devices.protocolVersionID", "devices.msgFieldDictionaryID", "devices.deviceDefinitionID", "devices.mobileNetworkID", "devices.longitude", "devices.latitude", "devices.timeStamp", "devices.ownerID", "devices.ownerUsername", "devices.ownerName", "devices.ownerEmail", "devices.devicePassword", "devices.oneWireVariables"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded devices1",{"trackPoint.position.longitude","trackPoint.position.latitude",
         "trackPoint.utc","trackPoint.valid","username","name",
         "devices.name","devices.imei","id"}),
    #"Removed Errors" = Table.RenameColumns(#"Removed Other Columns",
        {{"trackPoint.position.longitude","Longitude"},
         {"trackPoint.position.latitude","Latitude"},
         {"trackPoint.utc","utc"},
         {"trackPoint.valid","validity"},
         {"username","username"},
         {"name","name"},
         {"devices.name","device"},
         {"devices.imei","imei"},
         {"id","id"}}),
    #"Removed Columns" = Table.Distinct(#"Removed Errors", {"name"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Columns", each ([imei] <> null))
  in
    #"Filtered Rows"