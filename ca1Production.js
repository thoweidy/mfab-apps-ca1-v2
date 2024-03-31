const fs = require('fs');
const { Client } = require('pg');
const moment = require('moment');
const config = require('./config.json');
const { getData } = require('./wipJosDelta-API-Auth-Pull');
//const data = require('./data.json')

const { pushToSM357CheckpointWIP } = require('./smpush/357-Checkpoint-WIP');
const { pushToSM357DAMWIP } = require('./smpush/357D-AM-WIP');
const { pushToSM357BPropMELeadWIP } = require('./smpush/357B-Prop-ME-Lead-WIP');
const { pushToSM357NVRHoldWIP } = require('./smpush/357-NVR-Hold-WIP');
const { pushToSMQAEWIP } = require('./smpush/QAE-WIP');
const { pushToSM357DSMWIP } = require('./smpush/357D-SM-WIP');
const { pushToSM357DMETWIP } = require('./smpush/357D-MET-WIP');
const { pushToSM357JCableWIP } = require('./smpush/357J-Cable-WIP');
const { pushToSM357NDEWIP } = require('./smpush/357-NDE-WIP');
const { pushToSM357BPCFMELeadWIP } = require('./smpush/357B-PCF-ME-Lead-WIP');
const { pushToSM357JShieldWIP } = require('./smpush/357J-Shield-WIP');
const { pushToSMMPHTWIP } = require('./smpush/M-P-HT-WIP');
const { pushToSM357MCHARGEWIP } = require('./smpush/357-MCHARGE-WIP');
const { pushToSM357MEPrioritySheet } = require('./smpush/357-ME-Priority-Sheet');
const { pushToSM357OSPWIP } = require('./smpush/357-OSP-WIP');
const { pushToSM357REWORKWIP } = require('./smpush/357-REWORK-WIP');
const { pushToSM357JPaintWIP } = require('./smpush/357J-Paint-WIP');
const { pushToSM5128MechanicalInspectionWIP } = require('./smpush/5128-Mechanical-Inspection-WIP');
const { pushToSM357REMAKEWIP } = require('./smpush/357-REMAKE-WIP');
const { pushToSM357CJLFlightTechWIP } = require('./smpush/357CJL-Flight-Tech-WIP');
const { pushToSM357EMSWIP } = require('./smpush/357E-MS-WIP');
const { pushToSM357SOWIP } = require('./smpush/357-SO-WIP');


const client = new Client({
    host: 'jplis-dta-mfab.jpl.nasa.gov',
    database: 'mfabdatalake',
    port: 5432,
    user: 'thoweidy',
    password: 'mfabweidytho',
})

client.connect((err) => {
    if (err) {
        console.error('connection error', err.stack)
    } else {
        console.log('\x1b[47m\x1b[30mPostgreSQL database connection established and waiting to fetch data from the API. ...\x1b[0m\n');
    }
})

const smartsheet = require("smartsheet").createClient({
    accessToken: "VCh5Cy3RHWpTaPyuVkCUribMJWx3lF5EUdbeT",
    baseUrl: "https://api.smartsheetgov.com/2.0/",
    logLevel: "info",
});


/** Get Mapping from Smartsheet Mapping Doc 
 * Soon will be replaced with PostGRes Table that has all mappings
*/
const getMappingOpsfromSS = async () => {
    let operations = []
    const sheetId = 7680076806822004;
    const sheet = await smartsheet.sheets.getSheet({ id: sheetId });
    const rows = sheet.rows;
    let mainParentID = 0;
    rows.map((row) => {
        if (row.cells[0].value == 'Group') {
            mainParentID = row.id;
        }

        if (row.parentId == mainParentID && row.cells[0].value != 'Group') {
            operations.push({
                'Group': row.cells[0].value,
                'Id': row.id,
                'Resources': [],
            })
        }
    })

    operations.map((op) => {
        rows.map((row) => {
            if (row.parentId === op.Id && row.cells[0].value != 'Group') {
                op.Resources.push({
                    "Resource": row.cells[0].value,
                    "cagein": row.cells[1].value,
                    "cageout": row.cells[2].value,
                })
            }
        })
    })

    //fs.writeFileSync('operations.json', JSON.stringify(operations));

    return operations;
}

/** Push MFAB Data to PostGrs */
const pushMFABToPostGrs = async (data) => {

    console.log("-----------------------------------------------");
    console.log(`Pushing \x1b[33m${data.length}\x1b[0m Jobs of MFAB Data to PostGrs`);
    const wipJobs = data;
    let totalNewJobs = 0;
    let totalUpdatedJobs = 0;

    try {

        let completedJobs = 0;
        const progressBarWidth = 65;
        const totalOperations = wipJobs.reduce((total, job) => total + job.Operations.length, 0);

        for (job in wipJobs) {
            for (op in wipJobs[job].Operations) {

                /** Variables */
                const key = `'${wipJobs[job].Job_Number}_${wipJobs[job].Operations[op].Operation_Number}'`
                const job_Number = `'${wipJobs[job].Job_Number}'`
                const operation_Number = `'${wipJobs[job].Operations[op].Operation_Number}'`
                const operation_Description = wipJobs[job].Operations[op].Operation_Description === null ? null : `'${wipJobs[job].Operations[op].Operation_Description.replaceAll("'", "''")}'`
                const group = wipJobs[job].Operations[op].Group === null ? null : `'${wipJobs[job].Operations[op].Group}'`
                const oracle_Resource = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `'missing'` : wipJobs[job].Operations[op].Resources[0].Oracle_Resource === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Oracle_Resource}'`
                const estimated_Hours = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? 0 : wipJobs[job].Operations[op].Resources[0].Estimated_Hours === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Estimated_Hours}'`
                const quantity_Scheduled = wipJobs[job].Quantity_Scheduled === null ? null : `'${wipJobs[job].Quantity_Scheduled}'`
                const quantity_In_Queue = wipJobs[job].Operations[op].Quantity_In_Queue === null ? null : `'${wipJobs[job].Operations[op].Quantity_In_Queue}'`
                const quantity_Completed = wipJobs[job].Operations[op].Quantity_Completed === null ? null : `'${wipJobs[job].Operations[op].Quantity_Completed}'`
                const manufacturing_Engineer = wipJobs[job].Manufacturing_Engineer === null ? null : `'${wipJobs[job].Manufacturing_Engineer.replaceAll("'", "''")}'`
                const type = wipJobs[job].Type === null ? null : `'${wipJobs[job].Type}'`
                const oracle_Status = wipJobs[job].Oracle_Status === null ? null : `'${wipJobs[job].Oracle_Status}'`
                const flight = wipJobs[job].Flight === null ? null : `'${wipJobs[job].Flight}'`
                const overtime = wipJobs[job].Overtime === null ? null : `'${wipJobs[job].Overtime}'`
                const last_Move_Date = wipJobs[job].Operations[op].Last_Move_Date === null ? null : `'${wipJobs[job].Operations[op].Last_Move_Date}'`
                const part_Number = wipJobs[job].Part_Number === null ? null : `'${wipJobs[job].Part_Number}'`
                const revision = wipJobs[job].Revision === null ? null : `'${wipJobs[job].Revision}'`
                const part_Description = wipJobs[job].Part_Description === null ? null : `'${wipJobs[job].Part_Description.replaceAll("'", "''")}'`
                const promise_Date = wipJobs[job].Promise_Date === null ? null : `'${wipJobs[job].Promise_Date}'`
                const operation_Completed_Date = wipJobs[job].Operations[op].Operation_Completed_Date === null ? null : `'${wipJobs[job].Operations[op].Operation_Completed_Date}'`
                const customer = wipJobs[job].Customer === null ? null : `'${wipJobs[job].Customer.replaceAll("'", "''")}'`
                const customer_Project_Name = wipJobs[job].Customer_Project_Name === null ? null : `'${wipJobs[job].Customer_Project_Name}'`
                const customer_Project_Number = wipJobs[job].Customer_Project_Number === null ? null : `'${wipJobs[job].Customer_Project_Number}'`
                const customer_Task_Name = wipJobs[job].Customer_Task_Name === null ? null : `'${wipJobs[job].Customer_Task_Name}'`
                const customer_Task_Number = wipJobs[job].Customer_Task_Number === null ? null : `'${wipJobs[job].Customer_Task_Number}'`
                const estimated_Cost = wipJobs[job].Estimated_Cost === null ? null : `'${wipJobs[job].Estimated_Cost}'`
                const job_Last_Updated = wipJobs[job].Job_Last_Updated === null ? null : `'${wipJobs[job].Job_Last_Updated}'`
                const actual_Hours = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? 0 : wipJobs[job].Operations[op].Resources[0].Actual_Hours === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Actual_Hours}'`
                const baseline_Start = wipJobs[job].Operations[op].Baseline_Start === null ? null : `'${wipJobs[job].Operations[op].Baseline_Start}'`
                const baseline_Finish = wipJobs[job].Operations[op].Baseline_Finish === null ? null : `'${wipJobs[job].Operations[op].Baseline_Finish}'`
                const operation_Last_Updated = wipJobs[job].Operations[op].Operation_Last_Updated === null ? null : `'${wipJobs[job].Operations[op].Operation_Last_Updated}'`
                const resource_Sequence_Number = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `'missing'` : wipJobs[job].Operations[op].Resources[0].Resource_Sequence_Number === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Resource_Sequence_Number}'`
                const units = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `'missing'` : wipJobs[job].Operations[op].Resources[0].Units === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Units}'`
                /** ************ *****************  ******/


                await new Promise((resolve, reject) => {
                    client.query(`SELECT exists (SELECT 1 FROM lakeschema."Oracle_API_Source1" WHERE "Job_Op" = ${key} LIMIT 1);`, (err, res) => {
                        if (err) reject(err)

                        if (res.rows[0].exists === false) {
                            client.query(`INSERT INTO lakeschema."Oracle_API_Source1"(
                                "Job_Op", "Job_Number", "Operation_Number", "Operation_Description", "Group", "Oracle_Resource", "Estimated_Hours", "Quantity_Scheduled", "Quantity_In_Queue", "Quantity_Completed", "Manufacturing_Engineer", "Type", "Oracle_Status", "Flight", "Overtime", "Last_Move_Date", "Part_Number", "Revision", "Part_Description", "Promise_Date", "Operation_Completed_Date", "Customer", "Customer_Project_Name", "Customer_Project_Number", "Customer_Task_Name", "Customer_Task_Number", "Estimated_Cost", "Job_Last_Updated", "Actual_Hours", "Baseline_Start", "Baseline_Finish", "Operation_Last_Updated", "Resource_Sequence_Number", "Units")
                                VALUES (${key}, ${job_Number}, ${operation_Number}, ${operation_Description}, ${group}, ${oracle_Resource}, ${estimated_Hours}, ${quantity_Scheduled}, ${quantity_In_Queue}, ${quantity_Completed}, ${manufacturing_Engineer}, ${type}, ${oracle_Status}, ${flight}, ${overtime}, ${last_Move_Date}, ${part_Number}, ${revision}, ${part_Description}, ${promise_Date}, ${operation_Completed_Date}, ${customer}, ${customer_Project_Name}, ${customer_Project_Number}, ${customer_Task_Name}, ${customer_Task_Number}, ${estimated_Cost}, ${job_Last_Updated}, ${actual_Hours}, ${baseline_Start}, ${baseline_Finish}, ${operation_Last_Updated}, ${resource_Sequence_Number}, ${units});`, (err, res) => {
                                if (err) throw err
                            })
                            totalNewJobs++
                            completedJobs++;
                            const progress = Math.floor((completedJobs / totalOperations) * progressBarWidth);
                            const progressBar = "[" + "■".repeat(progress) + " ".repeat(progressBarWidth - progress) + "]";
                            process.stdout.write(`\r\x1b[37m${progressBar}\x1b[0m ${Math.floor((completedJobs / totalOperations) * 100)}% `);
                            resolve();
                        }

                        if (res.rows[0].exists === true) {

                            client.query(`UPDATE lakeschema."Oracle_API_Source1"
                                    SET "Job_Number"=${job_Number}, "Operation_Number"=${operation_Number}, "Operation_Description"=${operation_Description}, "Group"=${group}, "Oracle_Resource"=${oracle_Resource}, "Estimated_Hours"=${estimated_Hours}, "Quantity_Scheduled"=${quantity_Scheduled}, "Quantity_In_Queue"=${quantity_In_Queue}, "Quantity_Completed"=${quantity_Completed}, "Manufacturing_Engineer"=${manufacturing_Engineer}, "Type"=${type}, "Oracle_Status"=${oracle_Status}, "Flight"=${flight}, "Overtime"=${overtime}, "Last_Move_Date"=${last_Move_Date}, "Part_Number"=${part_Number}, "Revision"=${revision}, "Part_Description"=${part_Description}, "Promise_Date"=${promise_Date}, "Operation_Completed_Date"=${operation_Completed_Date}, "Customer"=${customer}, "Customer_Project_Name"=${customer_Project_Name}, "Customer_Project_Number"=${customer_Project_Number}, "Customer_Task_Name"=${customer_Task_Name}, "Customer_Task_Number"=${customer_Task_Number}, "Estimated_Cost"=${estimated_Cost}, "Job_Last_Updated"=${job_Last_Updated}, "Actual_Hours"=${actual_Hours}, "Baseline_Start"=${baseline_Start}, "Baseline_Finish"=${baseline_Finish}, "Operation_Last_Updated"=${operation_Last_Updated}, "Resource_Sequence_Number"=${resource_Sequence_Number}, "Units"=${units}, "Class_Code"=null, "Released_Date"=null, "Estimated_Value"=null, "Total_Actuals"=null
                                    WHERE "Job_Op"=${key};`, (err, res) => {
                                if (err) throw err
                            })
                            totalUpdatedJobs++
                            completedJobs++;
                            const progress = Math.floor((completedJobs / totalOperations) * progressBarWidth);
                            const progressBar = "[" + "■".repeat(progress) + " ".repeat(progressBarWidth - progress) + "]";
                            process.stdout.write(`\r\x1b[37m${progressBar}\x1b[0m ${Math.floor((completedJobs / totalOperations) * 100)}% `);
                            resolve();
                        }
                        resolve();
                    })
                });

            }
        }

    } catch (err) {
        console.error(err);
    }

    console.log(`\nNew Records: \x1b[31m${totalNewJobs}\x1b[0m`);
    console.log(`Updated Records: \x1b[31m${totalUpdatedJobs}\x1b[0m`);
    console.log("-----------------------------------\n");

}

/** Push Non-MFAB Data to PostGrs */
const pushNonMFABToPostGrs = async (data) => {
    console.log("-----------------------------------------------");
    console.log(`Pushing \x1b[33m${data.length}\x1b[0m Jobs of Non-MFAB Data to PostGrs`);


    const wipJobs = data;
    let totalNewJobs = 0;
    let totalUpdatedJobs = 0;

    try {

        let completedJobs = 0;
        const progressBarWidth = 65;
        const totalOperations = wipJobs.reduce((total, job) => total + job.Operations.length, 0);

        for (job in wipJobs) {
            for (op in wipJobs[job].Operations) {

                /** Variables */
                const key = `'${wipJobs[job].Job_Number}_${wipJobs[job].Operations[op].Operation_Number}'`
                const job_Number = `'${wipJobs[job].Job_Number}'`
                const operation_Number = `'${wipJobs[job].Operations[op].Operation_Number}'`
                const operation_Description = wipJobs[job].Operations[op].Operation_Description === null ? null : `'${wipJobs[job].Operations[op].Operation_Description.replaceAll("'", "''")}'`
                const group = wipJobs[job].Operations[op].Group === null ? null : `'${wipJobs[job].Operations[op].Group}'`
                const oracle_Resource = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `'missing'` : wipJobs[job].Operations[op].Resources[0].Oracle_Resource === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Oracle_Resource}'`
                const estimated_Hours = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? 0 : wipJobs[job].Operations[op].Resources[0].Estimated_Hours === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Estimated_Hours}'`
                const quantity_Scheduled = wipJobs[job].Quantity_Scheduled === null ? null : `'${wipJobs[job].Quantity_Scheduled}'`
                const quantity_In_Queue = wipJobs[job].Operations[op].Quantity_In_Queue === null ? null : `'${wipJobs[job].Operations[op].Quantity_In_Queue}'`
                const quantity_Completed = wipJobs[job].Operations[op].Quantity_Completed === null ? null : `'${wipJobs[job].Operations[op].Quantity_Completed}'`
                const manufacturing_Engineer = wipJobs[job].Manufacturing_Engineer === null ? null : `'${wipJobs[job].Manufacturing_Engineer.replaceAll("'", "''")}'`
                const type = wipJobs[job].Type === null ? null : `'${wipJobs[job].Type}'`
                const oracle_Status = wipJobs[job].Oracle_Status === null ? null : `'${wipJobs[job].Oracle_Status}'`
                const flight = wipJobs[job].Flight === null ? null : `'${wipJobs[job].Flight}'`
                const overtime = wipJobs[job].Overtime === null ? null : `'${wipJobs[job].Overtime}'`
                const last_Move_Date = wipJobs[job].Operations[op].Last_Move_Date === null ? null : `'${wipJobs[job].Operations[op].Last_Move_Date}'`
                const part_Number = wipJobs[job].Part_Number === null ? null : `'${wipJobs[job].Part_Number}'`
                const revision = wipJobs[job].Revision === null ? null : `'${wipJobs[job].Revision}'`
                const part_Description = wipJobs[job].Part_Description === null ? null : `'${wipJobs[job].Part_Description.replaceAll("'", "''")}'`
                const promise_Date = wipJobs[job].Promise_Date === null ? null : `'${wipJobs[job].Promise_Date}'`
                const operation_Completed_Date = wipJobs[job].Operations[op].Operation_Completed_Date === null ? null : `'${wipJobs[job].Operations[op].Operation_Completed_Date}'`
                const customer = wipJobs[job].Customer === null ? null : `'${wipJobs[job].Customer.replaceAll("'", "''")}'`
                const customer_Project_Name = wipJobs[job].Customer_Project_Name === null ? null : `'${wipJobs[job].Customer_Project_Name}'`
                const customer_Project_Number = wipJobs[job].Customer_Project_Number === null ? null : `'${wipJobs[job].Customer_Project_Number}'`
                const customer_Task_Name = wipJobs[job].Customer_Task_Name === null ? null : `'${wipJobs[job].Customer_Task_Name}'`
                const customer_Task_Number = wipJobs[job].Customer_Task_Number === null ? null : `'${wipJobs[job].Customer_Task_Number}'`
                const estimated_Cost = wipJobs[job].Estimated_Cost === null ? null : `'${wipJobs[job].Estimated_Cost}'`
                const job_Last_Updated = wipJobs[job].Job_Last_Updated === null ? null : `'${wipJobs[job].Job_Last_Updated}'`
                const actual_Hours = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? 0 : wipJobs[job].Operations[op].Resources[0].Actual_Hours === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Actual_Hours}'`
                const baseline_Start = wipJobs[job].Operations[op].Baseline_Start === null ? null : `'${wipJobs[job].Operations[op].Baseline_Start}'`
                const baseline_Finish = wipJobs[job].Operations[op].Baseline_Finish === null ? null : `'${wipJobs[job].Operations[op].Baseline_Finish}'`
                const operation_Last_Updated = wipJobs[job].Operations[op].Operation_Last_Updated === null ? null : `'${wipJobs[job].Operations[op].Operation_Last_Updated}'`
                const resource_Sequence_Number = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `'missing'` : wipJobs[job].Operations[op].Resources[0].Resource_Sequence_Number === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Resource_Sequence_Number}'`
                const units = wipJobs[job].Operations[op].hasOwnProperty('Resources') === false ? `'missing'` : wipJobs[job].Operations[op].Resources[0].Units === null ? null : `'${wipJobs[job].Operations[op].Resources[0].Units}'`
                /** ************ *****************  ******/

                await new Promise((resolve, reject) => {
                    client.query(`SELECT exists (SELECT 1 FROM lakeschema."NON_357_Oracle_API" WHERE "Job_Op" = ${key} LIMIT 1);`, (err, res) => {
                        if (err) reject(err)

                        if (res.rows[0].exists === false) {
                            client.query(`INSERT INTO lakeschema."NON_357_Oracle_API"(
                                "Job_Op", "Job_Number", "Operation_Number", "Operation_Description", "Group", "Oracle_Resource", "Estimated_Hours", "Quantity_Scheduled", "Quantity_In_Queue", "Quantity_Completed", "Manufacturing_Engineer", "Type", "Oracle_Status", "Flight", "Overtime", "Last_Move_Date", "Part_Number", "Revision", "Part_Description", "Promise_Date", "Operation_Completed_Date", "Customer", "Customer_Project_Name", "Customer_Project_Number", "Customer_Task_Name", "Customer_Task_Number", "Estimated_Cost", "Job_Last_Updated", "Actual_Hours", "Baseline_Start", "Baseline_Finish", "Operation_Last_Updated", "Resource_Sequence_Number", "Units")
                                VALUES (${key}, ${job_Number}, ${operation_Number}, ${operation_Description}, ${group}, ${oracle_Resource}, ${estimated_Hours}, ${quantity_Scheduled}, ${quantity_In_Queue}, ${quantity_Completed}, ${manufacturing_Engineer}, ${type}, ${oracle_Status}, ${flight}, ${overtime}, ${last_Move_Date}, ${part_Number}, ${revision}, ${part_Description}, ${promise_Date}, ${operation_Completed_Date}, ${customer}, ${customer_Project_Name}, ${customer_Project_Number}, ${customer_Task_Name}, ${customer_Task_Number}, ${estimated_Cost}, ${job_Last_Updated}, ${actual_Hours}, ${baseline_Start}, ${baseline_Finish}, ${operation_Last_Updated}, ${resource_Sequence_Number}, ${units});`, (err, res) => {
                                if (err) throw err
                            })
                            totalNewJobs++
                            completedJobs++;
                            const progress = Math.floor((completedJobs / totalOperations) * progressBarWidth);
                            const progressBar = "[" + "■".repeat(progress) + " ".repeat(progressBarWidth - progress) + "]";
                            process.stdout.write(`\r\x1b[37m${progressBar}\x1b[0m ${Math.floor((completedJobs / totalOperations) * 100)}% `);
                            resolve();
                        }

                        if (res.rows[0].exists === true) {

                            client.query(`UPDATE lakeschema."NON_357_Oracle_API"
                                    SET "Job_Number"=${job_Number}, "Operation_Number"=${operation_Number}, "Operation_Description"=${operation_Description}, "Group"=${group}, "Oracle_Resource"=${oracle_Resource}, "Estimated_Hours"=${estimated_Hours}, "Quantity_Scheduled"=${quantity_Scheduled}, "Quantity_In_Queue"=${quantity_In_Queue}, "Quantity_Completed"=${quantity_Completed}, "Manufacturing_Engineer"=${manufacturing_Engineer}, "Type"=${type}, "Oracle_Status"=${oracle_Status}, "Flight"=${flight}, "Overtime"=${overtime}, "Last_Move_Date"=${last_Move_Date}, "Part_Number"=${part_Number}, "Revision"=${revision}, "Part_Description"=${part_Description}, "Promise_Date"=${promise_Date}, "Operation_Completed_Date"=${operation_Completed_Date}, "Customer"=${customer}, "Customer_Project_Name"=${customer_Project_Name}, "Customer_Project_Number"=${customer_Project_Number}, "Customer_Task_Name"=${customer_Task_Name}, "Customer_Task_Number"=${customer_Task_Number}, "Estimated_Cost"=${estimated_Cost}, "Job_Last_Updated"=${job_Last_Updated}, "Actual_Hours"=${actual_Hours}, "Baseline_Start"=${baseline_Start}, "Baseline_Finish"=${baseline_Finish}, "Operation_Last_Updated"=${operation_Last_Updated}, "Resource_Sequence_Number"=${resource_Sequence_Number}, "Units"=${units}, "Class_Code"=null, "Released_Date"=null, "Estimated_Value"=null, "Total_Actuals"=null
                                    WHERE "Job_Op"=${key};`, (err, res) => {
                                if (err) throw err
                            })
                            totalUpdatedJobs++
                            completedJobs++;
                            const progress = Math.floor((completedJobs / totalOperations) * progressBarWidth);
                            const progressBar = "[" + "■".repeat(progress) + " ".repeat(progressBarWidth - progress) + "]";
                            process.stdout.write(`\r\x1b[37m${progressBar}\x1b[0m ${Math.floor((completedJobs / totalOperations) * 100)}% `);
                        }
                        resolve();
                    })
                });
            }
        }

    } catch (err) {
        console.error(err);
    }

    console.log(`\nNew Records: \x1b[31m${totalNewJobs}\x1b[0m`);
    console.log(`Updated Records: \x1b[31m${totalUpdatedJobs}\x1b[0m`);
    console.log("-----------------------------------\n");
}



/** Main Function */
const main = async () => {
    const startTime = new Date().getTime();

    const toDate = moment().format("YYYY-MM-DDTHH:mm:ss");
    const fromDate = config.lastUpdated;

    console.log(`\x1b[31mFetching Data from API \x1b[0m from \x1b[33m${fromDate}\x1b[0m to \x1b[33m${toDate}\x1b[0m`);

    //const data = await getData('2024-03-25T00:00:00', '2024-03-31T12:00:00');
    const data = await getData(fromDate, toDate);

    if (data === null) {

        const endTime = new Date().getTime();
        console.log(`\x1b[31mTotal Time: \x1b[33m${(endTime - startTime) / 1000}\x1b[0m seconds\n\n`);
        config.waitTime = true;
        fs.writeFileSync('./config.json', JSON.stringify(config, null, 2));
        client.end(); // Close the PostgreSQL connection
        process.exit(); // Exit the Node.js process

    }

    const ssOperations = await getMappingOpsfromSS();
    let totalPayLoad = data.WIP_Jobs.length;
    let i357Jobs = [];
    let non357Jobs = [];

    try {
        data.WIP_Jobs.forEach((job) => {
            const operations = job.Operations;
            const hasValidGroup = operations.some(oraOp => ssOperations.some(ssOp => ssOp.Group === oraOp.Group));
            const hasValidOperations = operations.some(oraOp => oraOp.Resources?.some(oraRes => ssOperations.some(ssOp => ssOp.Resources.some(ssRes => ssRes.Resource === oraRes.Oracle_Resource))));
            if (hasValidGroup && hasValidOperations) {
                i357Jobs.push(job);
            } else {
                non357Jobs.push(job);
            }
        });


    } catch (error) {
        console.log(error);
    }

    let total357Operations = 0;
    i357Jobs.forEach((job) => {
        total357Operations += job.Operations.length;
    });


    let totalNon357Operations = 0;
    non357Jobs.forEach((job) => {
        totalNon357Operations += job.Operations.length;
    });

    console.log("\n-----------------------------------")
    console.log("\x1b[42m\x1b[30m ORACLE Data Received \x1b[0m");
    console.log(`Total ORACLE PayLoad: \x1b[33m${totalPayLoad}\x1b[0m\n`);
    console.log(`Total ORACLE M357 Jobs: \x1b[31m${i357Jobs.length}\x1b[0m`);
    console.log(`Total Operations in M357 Jobs: \x1b[31m${total357Operations}\x1b[0m\n`);
    console.log(`Total ORACLE NON-M375 Jobs: \x1b[31m${non357Jobs.length}\x1b[0m`);
    console.log(`Total Operations in NON-M357 Jobs: \x1b[31m${totalNon357Operations}\x1b[0m`);
    console.log("-----------------------------------\n")


    await pushMFABToPostGrs(i357Jobs);
    await pushNonMFABToPostGrs(non357Jobs);

    await pushToSM357CheckpointWIP(i357Jobs);
    await pushToSM357MCHARGEWIP(i357Jobs);
    await pushToSM357MEPrioritySheet(i357Jobs);
    await pushToSM357NDEWIP(i357Jobs);
    await pushToSM357NVRHoldWIP(i357Jobs);
    await pushToSM357OSPWIP(i357Jobs);
    await pushToSM357SOWIP(i357Jobs);

    await pushToSM357REMAKEWIP(i357Jobs);
    await pushToSM357REWORKWIP(i357Jobs);

    await pushToSM357BPCFMELeadWIP(i357Jobs);
    await pushToSM357BPropMELeadWIP(i357Jobs);

    await pushToSM357CJLFlightTechWIP(i357Jobs);

    await pushToSM357DAMWIP(i357Jobs);
    await pushToSM357DMETWIP(i357Jobs);
    await pushToSM357DSMWIP(i357Jobs);

    await pushToSM357EMSWIP(i357Jobs); // Issue with one of the column valueTypes FIXED

    await pushToSM357JCableWIP(i357Jobs);
    await pushToSM357JShieldWIP(i357Jobs);
    await pushToSM357JPaintWIP(i357Jobs); // Issue with one of the column valueTypes FIXED


    await pushToSMQAEWIP(i357Jobs);
    await pushToSMMPHTWIP(i357Jobs);
    await pushToSM5128MechanicalInspectionWIP(i357Jobs); // Issue with one of the column valueTypes FIXED

    config.lastUpdated = toDate;
    fs.writeFileSync('./config.json', JSON.stringify(config, null, 2));

    const endTime = new Date().getTime();
    console.log(`\x1b[31mTotal Time: \x1b[33m${(endTime - startTime) / 1000}\x1b[0m seconds\n\n`);
    client.end(); // Close the PostgreSQL connection
    process.exit(); // Exit the Node.js process

}

main();