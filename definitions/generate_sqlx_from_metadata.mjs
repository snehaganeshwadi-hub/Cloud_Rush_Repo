import fs from "fs";
import path from "path";
import { parse } from "csv-parse/sync";
 
export function generateTables(dataform) {
  const metadataPath = path.resolve("./metadata/metadata.csv");
  const csvContent = fs.readFileSync(metadataPath, "utf8");
 
  const records = parse(csvContent, { columns: true });
 
  records.forEach(row => {
    const { entity, type, columns } = row;
    const cols = columns.split(",").map(c => c.trim());
 
    if (type === "hub") {
      dataform.table(`${entity}_hub`, {
        type: "table",
        columns: cols,
        description: `Hub table for ${entity}`
      }).query(ctx => `
        SELECT DISTINCT ${cols.join(", ")}
        FROM source_table.${entity}_staging
      `);
    }
 
    if (type === "sat") {
      dataform.table(`${entity}_sat`, {
        type: "table",
        columns: cols,
        description: `Satellite table for ${entity}`
      }).query(ctx => `
        SELECT ${cols.join(", ")}
        FROM source_table.${entity}_staging
      `);
    }
 
    if (type === "link") {
      dataform.table(`${entity}_link`, {
        type: "table",
        columns: cols,
        description: `Link table for ${entity}`
      }).query(ctx => `
        SELECT ${cols.join(", ")}
        FROM source_table.${entity}_staging
      `);
    }
  });
}