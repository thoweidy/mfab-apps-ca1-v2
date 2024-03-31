const fs = require('fs');
// Import sheetIds.json
const sheetIds = require('./sheetIdsv2.json');

const funcNames = [];


// Loop through the array
sheetIds.forEach((obj) => {
    if (!obj.sheetRealName.includes("Completed")) {
        const { fileName, sheetRealName } = obj;
    
    
        //Read the contents of app-template.js
        fs.readFile('./pushToSM.js', 'utf8', (err, data) => {
            if (err) {
                console.error(err);
                return;
            }
    
            // Duplicate app-template.js and rename it to the object key "fileName"
            const newFileName = `smpush/${fileName}.js`;
            const funcName = `pushToSM${fileName.replace(/-/g, '').charAt(0).toUpperCase() + fileName.replace(/-/g, '').slice(1)}`;
            funcNames.push({
                await: funcName,
                require: `require('./smpush/${fileName}');`
            });
            fs.writeFile(newFileName, data, 'utf8', (err) => {
                if (err) {
                    console.error(err);
                    return;
                }
    
                // Change the "sheetId" variable in the new file to object key "sheetID"
                const updatedData1 = data.replace(/const sheet = .+;/, `const sheet = '${sheetRealName}';`);
                
                // Change the function name in the new file
                // const updatedData2 = updatedData1.replace(/pushToSmartsheet.+?\(/, `${funcName}`);

                // const updatedData3 = updatedData1.replace(/pushToSmartsheet.+?\(/, `${funcName}(`);

                // Change the function name in the new file
                const updatedData2 = updatedData1.replace(/pushToSmartsheet/g, `${funcName}`);

                fs.writeFile(newFileName, updatedData2, 'utf8', (err) => {
                    if (err) {
                        console.error(err);
                        return;
                    }

                    console.log(`File ${newFileName} has been created and updated successfully.`);
                });

            });
        });
    }
});

setTimeout(() => {
    fs.writeFile('funcNames.json', JSON.stringify(funcNames), 'utf8', (err) => {
        if (err) {
            console.error(err);
            return;
        }
    
        console.log(`File funcNames.json has been created successfully.`);
    });
}, 2000);