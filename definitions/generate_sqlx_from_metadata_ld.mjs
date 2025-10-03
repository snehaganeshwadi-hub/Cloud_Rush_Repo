// 📁 File: definitions/generate_dv_tables.js
 
const fs = require("fs");
const path = require("path");
 
const metadataPath = path.resolve(__Metadata, "metadata.csv");
const metadataCsv = fs.readFileSync("Metadata/metadata.csv", "utf8");
 
// Parse CSV manually
function parseCSV(csv) {
  const [headerLine, ...lines] = csv.trim().split("\n");
  const headers = headerLine.split(",");
  return lines.map(line => {
    const values = line.split(",");
    const entry = {};
    headers.forEach((header, idx) => {
      entry[header.trim()] = values[idx]?.trim();
    });
    return entry;
  });
}
 
const metadata = parseCSV(metadataCsv);
 
// Helper function to format column list
const formatColumnList = colStr =>
  colStr.split(";").map(c => c.trim()).filter(Boolean);
 
// -- MAIN LOOP --
metadata.forEach(row => {
  const entityType = row.entity_type.toLowerCase();
  const entityName = row.entity_name;
  const businessKey = row.business_key;
  const relatedEntities = row.related_entity;
  const columns = formatColumnList(row.columns);
  const schemaName = "Cloud_Rush_dataset";
  const recordSource = "'AirIndia'";
 
  // Generate hash key
  const hk = `hk_${entityName}`;
 
  if (entityType === "hub") {
    publish(`hub_${entityName}`)
      .schema(schemaName)
      .type("incremental")
      .description(`Hub table for ${entityName}`)
      .tags(["hub", "datavault"])
      .columns([
        { name: hk, description: `Hash key for ${businessKey}` },
        { name: businessKey, description: "Business key" },
        { name: "load_date", description: "Load timestamp" },
        { name: "record_source", description: "Source system" }
      ])
      .query(`
        SELECT
          MD5(${businessKey}) AS ${hk},
          ${businessKey},
          CURRENT_TIMESTAMP() AS load_date,
          ${recordSource} AS record_source
        FROM ${ref(`staging.${entityName}_raw`)}
      `);
  }
 
  if (entityType === "satellite" || entityType === "hub") {
    publish(`sat_${entityName}_details`)
      .schema(schemaName)
      .type("incremental")
      .description(`Satellite for ${entityName} attributes`)
      .tags(["satellite", "datavault"])
      .columns([
        { name: hk, description: "Hash key (FK to hub)" },
        { name: "hashdiff", description: "Hashdiff of attributes" },
        { name: "load_date", description: "Load timestamp" },
        { name: "record_source", description: "Source system" },
        ...columns.map(col => ({
          name: col,
          description: `${col}`
        }))
      ])
      .query(`
        SELECT
          MD5(${businessKey}) AS ${hk},
          MD5(CONCAT(
            ${columns.map(col => `COALESCE(CAST(${col} AS STRING), '')`).join(",\n            ")}
          )) AS hashdiff,
          CURRENT_TIMESTAMP() AS load_date,
          ${recordSource} AS record_source,
          ${columns.join(", ")}
        FROM ${ref(`staging.${entityName}_raw`)}
      `);
  }
 
  if (entityType === "link") {
    const relEntities = relatedEntities.split("|");
    const relHKs = relEntities.map(e => `MD5(${e}_id) AS hk_${e}`).join(",\n        ");
 
    publish(`link_${entityName}`)
      .schema(schemaName)
      .type("incremental")
      .description(`Link table for ${entityName}`)
      .tags(["link", "datavault"])
      .columns([
        { name: `hk_${entityName}`, description: "Hash key for link" },
        ...relEntities.map(e => ({
          name: `hk_${e}`,
          description: `Hash key for ${e}`
        })),
        { name: businessKey, description: "Link business key" },
        { name: "load_date", description: "Load timestamp" },
        { name: "record_source", description: "Source system" }
      ])
      .query(`
        SELECT
          MD5(${businessKey}) AS hk_${entityName},
          ${relHKs},
          ${businessKey},
          CURRENT_TIMESTAMP() AS load_date,
          ${recordSource} AS record_source
        FROM ${ref(`staging.${entityName}_raw`)}
      `);
  }
});