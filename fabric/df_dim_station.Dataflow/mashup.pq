[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_station_dfg2_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "station_id", DestinationColumnName = "station_id"], [SourceColumnName = "station_first_seen_at", DestinationColumnName = "station_first_seen_at"], [SourceColumnName = "station_last_seen_at", DestinationColumnName = "station_last_seen_at"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "physical_configuration", DestinationColumnName = "physical_configuration"], [SourceColumnName = "lat", DestinationColumnName = "lat"], [SourceColumnName = "lon", DestinationColumnName = "lon"], [SourceColumnName = "altitude", DestinationColumnName = "altitude"], [SourceColumnName = "address", DestinationColumnName = "address"], [SourceColumnName = "district_code", DestinationColumnName = "district_code"], [SourceColumnName = "district", DestinationColumnName = "district"], [SourceColumnName = "neighbourhood_code", DestinationColumnName = "neighbourhood_code"], [SourceColumnName = "neighbourhood", DestinationColumnName = "neighbourhood"], [SourceColumnName = "post_code", DestinationColumnName = "post_code"], [SourceColumnName = "is_charging_station", DestinationColumnName = "is_charging_station"], [SourceColumnName = "station_kind", DestinationColumnName = "station_kind"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_station_dfg2 = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a49f3c43-0b7b-4eeb-8ee3-83e6d027f990"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "77fdc15c-c9d6-4cdc-b0e1-daa5fbf08ea6"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver.station_info", ItemKind = "Table"]}[Data],
  #"Grouped rows" = Table.Group(#"Navigation 2", {"station_id"}, {{"station_first_seen_at", each List.Min([first_seen_at]), type nullable datetime}, {"station_last_seen_at", each List.Max([last_seen_at]), type nullable datetime}, {"versions", each _, type nullable table[station_id = nullable Int64.Type, version_no = nullable Int64.Type, first_seen_at = nullable datetime, last_seen_at = nullable datetime, snapshots = nullable Int64.Type, name = nullable text, physical_configuration = nullable text, lat = nullable number, lon = nullable number, altitude = nullable number, address = nullable text, district_code = nullable text, district = nullable text, neighbourhood_code = nullable text, neighbourhood = nullable text, post_code = nullable text, capacity = nullable Int64.Type, is_charging_station = nullable logical, short_name = nullable text]}}),
  #"Added custom" = Table.AddColumn(#"Grouped rows", "latest", each Table.Max([versions], "version_no")),
  #"Expanded latest" = Table.ExpandRecordColumn(#"Added custom", "latest", {"name", "physical_configuration", "lat", "lon", "altitude", "address", "district_code", "district", "neighbourhood_code", "neighbourhood", "post_code", "is_charging_station"}, {"name", "physical_configuration", "lat", "lon", "altitude", "address", "district_code", "district", "neighbourhood_code", "neighbourhood", "post_code", "is_charging_station"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded latest", {"versions"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"lat", type number}, {"lon", type number}, {"altitude", type number}, {"address", type text}, {"district_code", type text}, {"district", type text}, {"neighbourhood_code", type text}, {"neighbourhood", type text}, {"post_code", type text}, {"is_charging_station", type logical}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "station_kind", each if Text.Contains([name], "testing", Comparer.OrdinalIgnoreCase)
   or Text.Contains([name], "pruebas", Comparer.OrdinalIgnoreCase)
then "Test"
else if [physical_configuration] = "VALET"
then "Event"
else "Regular"), {{"station_kind", type text}}),
  #"Replaced value" = Table.ReplaceValue(#"Added custom 1", null, "Unassigned", Replacer.ReplaceValue, {"district", "neighbourhood"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value", {{"name", type text}, {"physical_configuration", type text}})
in
  #"Changed column type 1";
shared dim_station_dfg2_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "a49f3c43-0b7b-4eeb-8ee3-83e6d027f990"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "77fdc15c-c9d6-4cdc-b0e1-daa5fbf08ea6"]}[Data],
  Navigation_3 = Navigation_2{[Id = "gold", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "dim_station_dfg2", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
