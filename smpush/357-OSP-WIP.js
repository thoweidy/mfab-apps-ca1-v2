const fs = require('fs');
const moment = require('moment');
//const data = require('../data.json')
/** Push to SmartSheet */
const pushToSM357OSPWIP = async (data) => {

    return new Promise(async (resolve, reject) => {
        const sheet = '357 OSP WIP';

        /**  Sheet Variables */
        const mapping = require('../sheetIdsv2.json');
        const wipJobs = data
        const wipSheetId = mapping.find(sh => sh.sheetRealName === sheet) ? mapping.find(sh => sh.sheetRealName === sheet).sheetID : null;
        const wipSheetName = mapping.find(sh => sh.sheetRealName === sheet) ? mapping.find(sh => sh.sheetRealName === sheet).sheetName : null;
        const wipSheetKey = mapping.find(sh => sh.sheetRealName === sheet) ? mapping.find(sh => sh.sheetRealName === sheet).apiKey : null;
        const wipGroups = mapping.find(sh => sh.sheetRealName === sheet) ? mapping.find(sh => sh.sheetRealName === sheet).groups : null;
        const wipResource = mapping.find(sh => sh.sheetRealName === sheet) ? mapping.find(sh => sh.sheetRealName === sheet).resources : null;
        const completedSheetName = mapping.find(sh => sh.sheetRealName === sheet) ? mapping.find(sh => sh.sheetRealName === sheet).completeSheet : null;
        const completedSheetId = mapping.find(sh => sh.sheetRealName === completedSheetName) ? mapping.find(sh => sh.sheetRealName === completedSheetName).sheetID : null;
        const columns = mapping.find(sh => sh.sheetRealName === sheet) ? mapping.find(sh => sh.sheetRealName === sheet).columns : null;

        console.log("\n-----------------------------------------------")
        console.log("\x1b[42m\x1b[30m SHEET Information \x1b[0m");
        console.log("Pushing to Smartsheet WIP: \x1b[47m\x1b[30m " + wipSheetName + " \x1b[0m");
        console.log("Pushing to Smartsheet WIP Completed: \x1b[47m\x1b[30m " + completedSheetName + " \x1b[0m");
        console.log("Groups To look for: ", wipGroups);
        console.log("Resources To look for: ", wipResource);
        console.log("-----------------------------------------------\n")

        /** Smartsheet API Connection */
        const smartsheet = require("smartsheet").createClient({
            accessToken: `${wipSheetKey}`,
            baseUrl: "https://api.smartsheetgov.com/2.0/",
            logLevel: "info",
        });


        /** Get Required Columns */
        /** Get Required Columns */
        const wipCol = await smartsheet.sheets.getColumns({
            sheetId: wipSheetId,
            queryParameters: {
                includeAll: true
            }
        });
        const wipcolIds = wipCol.data.reduce((acc, col) => {
            acc.push({ [col.title]: col.id });
            return acc;
        }, []);
        //fs.writeFileSync('wipcolIds.json', JSON.stringify(wipcolIds));

        const completedWipCol = completedSheetName === null ? [] : await smartsheet.sheets.getColumns({ sheetId: completedSheetId, queryParameters: { includeAll: true } });
        const completedWipcolIds = completedSheetName === null ? [] : completedWipCol.data.reduce((acc, col) => {
            acc.push({ [col.title]: col.id });
            return acc;
        }, []);
        //fs.writeFileSync('completedWipcolIds.json', JSON.stringify(completedWipcolIds));

        /** Get WIP and Completed Sheets Current Records */
        const wipSheet = await smartsheet.sheets.getSheet({ id: wipSheetId });
        const completedSheet = completedSheetName === null ? [] : await smartsheet.sheets.getSheet({ id: completedSheetId });

        /** Generate the Values to be updated in Smartsheet */
        /** ------------------------------------------------*/

        let ops = [];
        for (job in wipJobs) {
            for (op in wipJobs[job].Operations) {
                for (res in wipJobs[job].Operations[op].Resources) {
                    /** Variables */
                    if (wipGroups.includes(wipJobs[job].Operations[op].Group) && wipResource.includes(wipJobs[job].Operations[op].Resources[res].Oracle_Resource)) {
                        ops.push({
                            "Key": `${wipJobs[job].Job_Number}_${wipJobs[job].Operations[op].Operation_Number}`,
                            "Job Number": `${wipJobs[job].Job_Number}`,
                            "Operation Number": `${wipJobs[job].Operations[op].Operation_Number}`,
                            "Operation Description": wipJobs[job].Operations[op].Operation_Description === null ? null : `${wipJobs[job].Operations[op].Operation_Description.replaceAll("'", "''")}`,
                            "Group": wipJobs[job].Operations[op].Group === null ? null : `${wipJobs[job].Operations[op].Group}`,
                            "Oracle Resource": wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `missing` : wipJobs[job].Operations[op].Resources[res].Oracle_Resource === null ? null : `${wipJobs[job].Operations[op].Resources[res].Oracle_Resource}`,
                            "Estimated Hours": wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? 0 : wipJobs[job].Operations[op].Resources[0].Estimated_Hours === null ? null : `${wipJobs[job].Operations[op].Resources[0].Estimated_Hours}`,
                            "Quantity Scheduled": wipJobs[job].Quantity_Scheduled === null ? null : `${wipJobs[job].Quantity_Scheduled}`,
                            "Quantity In Queue": wipJobs[job].Operations[op].Quantity_In_Queue === null ? null : `${wipJobs[job].Operations[op].Quantity_In_Queue}`,
                            "Quantity Completed": wipJobs[job].Operations[op].Quantity_Completed === null ? null : `${wipJobs[job].Operations[op].Quantity_Completed}`,
                            "Manufacturing Engineer": wipJobs[job].Manufacturing_Engineer === null ? null : `${wipJobs[job].Manufacturing_Engineer.replaceAll("'", "''")}`,
                            "Flight": wipJobs[job].Flight === null ? null : `${wipJobs[job].Flight}`,
                            "Overtime": wipJobs[job].Overtime === null ? null : `${wipJobs[job].Overtime}`,
                            "Part Number": wipJobs[job].Part_Number === null ? null : `${wipJobs[job].Part_Number}`,
                            "Revision": wipJobs[job].Revision === null ? null : `${wipJobs[job].Revision}`,
                            "Part Description": wipJobs[job].Part_Description === null ? null : `${wipJobs[job].Part_Description.replaceAll("'", "''")}`,
                            "Operation Completed Date": wipJobs[job].Operations[op].Operation_Completed_Date === null ? null : `${moment(wipJobs[job].Operations[op].Operation_Completed_Date).format('YYYY-MM-DD')}`,
                            "Customer": wipJobs[job].Customer === null ? null : `${wipJobs[job].Customer.replaceAll("'", "''")}`,
                            "Customer Project Name": wipJobs[job].Customer_Project_Name === null ? null : `${wipJobs[job].Customer_Project_Name}`,
                            "Customer Project Number": wipJobs[job].Customer_Project_Number === null ? null : `${wipJobs[job].Customer_Project_Number}`,
                            "Customer Task Name": wipJobs[job].Customer_Task_Name === null ? null : `${wipJobs[job].Customer_Task_Name}`,
                            "Customer Task Number": wipJobs[job].Customer_Task_Number === null ? null : `${wipJobs[job].Customer_Task_Number}`,
                            "Actual Hours": wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? 0 : wipJobs[job].Operations[op].Resources[0].Actual_Hours === null ? null : `${wipJobs[job].Operations[op].Resources[0].Actual_Hours}`,
                            "Baseline Start": wipJobs[job].Operations[op].Baseline_Start === null ? null : `${moment(wipJobs[job].Operations[op].Baseline_Start).format('YYYY-MM-DD')}`,
                            "Baseline Finish": wipJobs[job].Operations[op].Baseline_Finish === null ? null : `${moment(wipJobs[job].Operations[op].Baseline_Finish).format('YYYY-MM-DD')}`,
                            "Units": wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `missing` : wipJobs[job].Operations[op].Resources[0].Units === null ? null : `${wipJobs[job].Operations[op].Resources[0].Units}`,
                            // "estimated_Cost": wipJobs[job].Estimated_Cost === null ? null : `${wipJobs[job].Estimated_Cost}`, //Need to check
                            // "promise_Date": wipJobs[job].Promise_Date === null ? null : `${wipJobs[job].Promise_Date}`, //Need to check
                            // "last_Move_Date": wipJobs[job].Operations[op].Last_Move_Date === null ? null : `${wipJobs[job].Operations[op].Last_Move_Date}`, //Need to check
                            // "oracle_Status": wipJobs[job].Oracle_Status === null ? null : `${wipJobs[job].Oracle_Status}`,//Need to check
                            // "job_Last_Updated": wipJobs[job].Job_Last_Updated === null ? null : `${wipJobs[job].Job_Last_Updated}`, //Need to check
                            // "operation_Last_Updated": wipJobs[job].Operations[op].Operation_Last_Updated === null ? null : `${wipJobs[job].Operations[op].Operation_Last_Updated}`, //Need to check
                            // "resource_Sequence_Number": wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `missing` : wipJobs[job].Operations[op].Resources[0].Resource_Sequence_Number === null ? null : `${wipJobs[job].Operations[op].Resources[0].Resource_Sequence_Number}`, //Need to check
                            // "type": wipJobs[job].Type === null ? null : `${wipJobs[job].Type}`, //Need to check
                        })
                    }
                    /** ************ *****************  ******/
                }
            }
        }

        console.log("\n-----------------------------------------------")
        console.log("\x1b[42m\x1b[30m Processing Data from ORACLE and MACTHING with SS \x1b[0m");
        console.log("\nTotal Operations Found: ", ops.length);

        /** Check for Duplicate Keys */
        const duplicateKeys = ops.filter((item, index) => ops.findIndex(i => i["Key"] === item["Key"]) !== index);
        if (duplicateKeys.length > 0) {
            console.log("Duplicate keys found in OPS array:");
            console.log(duplicateKeys);
        } else {
            console.log("No duplicate keys found in OPS array.");
        }
        /** ****************************************************************** */

        /** Get Available Column ID */
        const wipColIds = wipcolIds.filter((col) => columns.includes(Object.keys(col)[0]));
        const completedColIds = completedWipcolIds.filter((col) => columns.includes(Object.keys(col)[0]));

        fs.writeFileSync(`./jsons/${wipSheetName}-ColIds.json`, JSON.stringify(wipColIds));
        completedSheetName !== null && fs.writeFileSync(`./jsons/${completedSheetName.replace(/[\/\s]/g, "-")}-ColIds.json`, JSON.stringify(completedColIds));

        /** find matching operations in completedSheet */
        const wipOps = [];
        const completedOps = [];
        const newOps = [];

        for (op in ops) {
            const wipOp = wipSheet.rows.find((row) => row.cells.find((cell) => cell.value === ops[op].Key));
            const completedOp = completedSheetName === null ? undefined : completedSheet.rows.find((row) => row.cells.find((cell) => cell.value === ops[op].Key));
            if (wipOp) {
                wipOps.push(ops[op]);
            } else if (completedOp) {
                completedOps.push(ops[op]);
            } else {
                newOps.push(ops[op]);
            }
        }

        console.log("\nTotal New Operations: ", newOps.length);
        console.log("Total Operations in WIP Sheet: ", wipOps.length);
        console.log("Total Operations in Completed Sheet: ", completedOps.length);
        console.log("-----------------------------------------------\n");
        console.log("TOTAL: ", newOps.length + wipOps.length + completedOps.length + "\n");

        const newrows = newOps.map((op) => {
            return {
                toBottom: true,
                cells: columns.map((col) => {
                    return {
                        columnId: wipColIds.find((colId) => Object.keys(colId)[0] === col)[col],
                        value: op[col] === null ? null : op[col],
                        strict: false
                    }
                })
            }
        })

        const wiprows = wipOps.map((op) => {
            return {
                id: wipSheet.rows.find((row) => row.cells.find((cell) => cell.value === op.Key)).id,
                cells: columns.map((col) => {
                    return {
                        columnId: wipColIds.find((colId) => Object.keys(colId)[0] === col)[col],
                        value: op[col] === null ? null : op[col],
                        strict: false
                    }
                })
            }
        })

        const completedrows = completedOps.map((op) => {
            return {
                id: completedSheetName === null ? null : completedSheet.rows.find((row) => row.cells.find((cell) => cell.value === op.Key)).id,
                cells: columns.map((col) => {
                    return {
                        columnId: completedColIds.find((colId) => Object.keys(colId)[0] === col)[col],
                        value: op[col] === null ? null : op[col],
                        strict: false
                    }
                })
            }
        })

        fs.writeFileSync(`./jsons/${wipSheetName}-newrows.json`, JSON.stringify(newrows));
        fs.writeFileSync(`./jsons/${wipSheetName}-wiprows.json`, JSON.stringify(wiprows));
        completedSheetName !== null && fs.writeFileSync(`./jsons/${completedSheetName.replace(/[\/\s]/g, "-")}-completedrows.json`, JSON.stringify(completedrows));

        /** Push New Operations */
        try {
            const newresponse = await smartsheet.sheets.addRows({
                sheetId: wipSheetId,
                body: newrows
            })
            console.log("\x1b[47m\x1b[30m New Operations Added to WIP Sheet: \x1b[0m", newresponse.result.length, "\n");
        }
        catch (error) {
            console.log("Error Adding New Operations: ", error);
        }

        
        /** Update WIP Operations */
        try {
            const wipresponse = await smartsheet.sheets.updateRow({
                sheetId: wipSheetId,
                body: wiprows
            })                
            console.log("\x1b[47m\x1b[30m WIP Operations Updated in WIP Sheet: \x1b[0m", wipresponse.result.length, "\n");
        } catch (error) {
            console.log("Error Updating WIP Operations: ", error);
        }
            
        /** Update Completed Operations */
        if (completedSheetName !== null){
            try {
                const completedresponse = await smartsheet.sheets.updateRow({
                    sheetId: completedSheetId,
                    body: completedrows
                })
                console.log("\x1b[47m\x1b[30m Completed Operations Updated to Completed sheet: \x1b[0m", completedresponse.result.length, "\n");
            } catch (error) {
                console.log("Error Updating Completed Operations: ", error);
            }
        }

        console.log("\n-----------------------------------------------")
        console.log(`\x1b[42m\x1b[30m Completed Pushing ${sheet} to Smartsheet \x1b[0m`);
        console.log("-----------------------------------------------\n")
    resolve();

    });
}

module.exports = {
    pushToSM357OSPWIP
};