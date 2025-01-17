const fs = require("fs");
const csv = require("csv-parser");
const { parse } = require("json2csv");
const path = require("path");

// Function to parse CSV files
function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv({ separator: ";" }))
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", (err) => reject(err));
  });
}

// Function to load data from multiple CSV files
async function loadData() {
  const csvFiles = [
    "./data/bnu-lt-data.csv",
    "./data/apo-lt-data.csv",
    "./data/azt-lt-data.csv",
    "./data/cma-lt-data.csv",
    "./data/gin-lt-data.csv",
    "./data/ntn-lt-data.csv",
  ];

  let apoData = [];

  for (const file of csvFiles) {
    const data = await parseCSV(file);
    apoData = apoData.concat(data);
  }

  const matches = await parseCSV("./data/matches.csv");

  return { apoData, matches };
}

// Function to map related manufacturers
function mapRelatedManufacturers(matches, apoData) {
  const manufacturerMap = {};

  matches.forEach((match) => {
    const mainProduct = apoData.find(
      (p) =>
        p.source_id.trim() === match.m_source_id.trim() &&
        p.source.trim() === match.m_source.trim()
    );
    const compProduct = apoData.find(
      (p) =>
        p.source_id.trim() === match.c_source_id.trim() &&
        p.source.trim() === match.c_source.trim()
    );

    if (mainProduct && compProduct) {
      const mainManufacturer = mainProduct.manufacturer;
      const compManufacturer = compProduct.manufacturer;

      if (mainManufacturer && compManufacturer) {
        if (!manufacturerMap[mainManufacturer]) {
          manufacturerMap[mainManufacturer] = new Set();
        }
        manufacturerMap[mainManufacturer].add(compManufacturer);

        if (!manufacturerMap[compManufacturer]) {
          manufacturerMap[compManufacturer] = new Set();
        }
        manufacturerMap[compManufacturer].add(mainManufacturer);
      }
    }
  });

  return manufacturerMap;
}

// Function to assign relation types
function assignRelationType(manufacturerMap) {
  const relations = [];

  Object.keys(manufacturerMap).forEach((parent) => {
    manufacturerMap[parent].forEach((child) => {
      relations.push({ parent, child, relation_type: "sibling" });
    });
  });

  return relations;
}

// Function to save mapped data to CSV
function saveToCSV(manufacturerRelations) {
  if (manufacturerRelations.length === 0) {
    console.error("No manufacturer relations found to save.");
    return;
  }

  const outputDir = path.resolve("./output");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
  }

  const fields = ["parent", "child", "relation_type"];
  const csvData = parse(manufacturerRelations, { fields });

  fs.writeFileSync(path.join(outputDir, "manufacturerRelations.csv"), csvData);
  console.log("CSV file has been saved.");
}

// Function to assign a manufacturer based on the title
function assignManufacturerByTitle(title, apoData, manufacturerMap) {
  // Find the product with the given title
  const product = apoData.find(
    (p) => p.title.trim().toLowerCase() === title.trim().toLowerCase()
  );

  if (!product) {
    console.log(`No product found with the title: ${title}`);
    return null;
  }

  // Check if the product already has a manufacturer
  if (product.manufacturer && product.manufacturer.trim() !== "") {
    console.log(
      `Manufacturer already assigned for product "${title}": ${product.manufacturer}`
    );
    return product.manufacturer;
  }

  // If no manufacturer is assigned, attempt to use related manufacturers
  let possibleManufacturer = null;

  // Look for other products with the same title but with a manufacturer assigned
  const similarProducts = apoData.filter(
    (p) =>
      p.title.trim().toLowerCase() === title.trim().toLowerCase() &&
      p.manufacturer &&
      p.manufacturer.trim() !== ""
  );

  // If similar products with manufacturers are found, use their manufacturer
  if (similarProducts.length > 0) {
    possibleManufacturer = similarProducts[0].manufacturer; // Use the first match
    console.log(
      `Assigned Manufacturer for product "${title}": ${possibleManufacturer}`
    );
    return possibleManufacturer;
  }

  console.log(`No manufacturer found for product "${title}".`);
  return null;
}

// Main function to execute the process
async function main() {
  console.log("Processing data, please wait...");

  const { apoData, matches } = await loadData();

  const manufacturerMap = mapRelatedManufacturers(matches, apoData);

  const manufacturerRelations = assignRelationType(manufacturerMap);

  saveToCSV(manufacturerRelations);

  console.log("Processing complete.");

  const exampleTitle = "Libellys Dermo-Sensitive sauskelnės S (3-6 kg) 30 Vnt."; // Replace with actual product title
  const assignedManufacturer = assignManufacturerByTitle(
    exampleTitle,
    apoData,
    manufacturerMap
  );
  console.log(
    `Assigned Manufacturer for "${exampleTitle}":`,
    assignedManufacturer
  );
}

main();
