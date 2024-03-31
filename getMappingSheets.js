const { Client } = require('pg');
const fs = require('fs');

// Create a new PostgreSQL client
const client = new Client({
    host: 'jplis-dta-mfab.jpl.nasa.gov',
    database: 'mfabdatalake',
    port: 5432,
    user: 'thoweidy',
    password: 'mfabweidytho',
});

// Connect to the PostgreSQL database
client.connect();

const getMappingSheets = async () => {
    // Query the "SS_Sheet_IDs_StaticMap" table and select "Sheet_Name" and "SmartSheet_Sheet_ID"
    const query = 'SELECT "Sheet_Name", "SmartSheet_Sheet_ID", "Group", "Resources", "API_Key", "Req_Column", "Complete_Sheet" FROM lakeschema."SS_Sheet_IDs_StaticMap"';

    client.query(query, (err, result) => {
        if (err) {
            console.error('Error executing query:', err);
            client.end();
            return;
        }

        // Loop through the records and create a JSON array
        const jsonArray = result.rows.map(row => {
            const sheetRealName= row.Sheet_Name;
            const sheetName = row.Sheet_Name.replace(/[\s\W-]+/g, "-");
            const completeSheet = row.Complete_Sheet;
            const sheetID = row.SmartSheet_Sheet_ID;
            const fileName = sheetName;
            const groups = row.Group.split(",");
            const resources = row.Resources.split(",");
            const apiKey = row.API_Key;
            const columns = row.Req_Column.split(",");
            return { sheetRealName, sheetName, completeSheet, sheetID, fileName, groups, resources, apiKey, columns };
        });


        // Export the JSON records to a file named "sheetIdsv2.json"
        fs.writeFile('sheetIdsv2.json', JSON.stringify(jsonArray), err => {
            if (err) {
                console.error('Error exporting JSON:', err);
            } else {
                console.log('JSON exported successfully!');
            }
            client.end();
        });

    });
}

getMappingSheets();
