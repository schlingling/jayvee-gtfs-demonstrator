import * as fs from "fs";
import * as GtfsRealtimeBindings from "gtfs-realtime-bindings";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import csv from "csv-parser";
import { exec } from "child_process";

const PATH_PIPELINE_DB = "resources/gtfs-static-and-rt_pipeline-result.sqlite";

export class GtfsDemonstrator {
  public async validateAlerts(pathToRawFile: string, pathToDB: string): Promise<[number, number, number]> {
    const sqlStatement = `SELECT * FROM rt_alert WHERE [entity.id] = ? AND [entity.alert.informed_entity.route_id] = ?`;
    const db = await this.initDatabase(pathToDB);
    const feed = this.initFeedMessage(pathToRawFile);
    let counterEntities = 0;
    const countRows = (await db.get(`SELECT  COUNT(*) count FROM rt_alert`)).count;
    let counterMatches = 0;
    for (const entity of feed.entity) {
      for (const informedEntity of entity.alert?.informedEntity as GtfsRealtimeBindings.transit_realtime.IEntitySelector[]) {
        const rows = await db.all(sqlStatement, [entity.id, informedEntity?.routeId]);
        counterEntities++;
        if (rows.length === 1) {
          counterMatches++;
        }
      }
    }
    return [counterEntities, countRows, counterMatches];
  }
  public async validateVehiclePositions(pathToRawFile: string, pathToDB: string): Promise<[number, number, number]> {
    const feed = this.initFeedMessage(pathToRawFile);
    const sqlStatement = `SELECT * FROM rt_vehicle_position WHERE [entity.id] = ? AND [entity.vehicle_position.vehicle_descriptor.id] = ? AND [entity.vehicle_position.trip.trip_id] = ? AND [entity.vehicle_position.trip.route_id] = ?`;
    const db = await this.initDatabase(pathToDB);

    let counterEntities = 0;
    const countRows = (await db.get(`SELECT  COUNT(*) count FROM rt_vehicle_position`)).count;
    let counterMatches = 0;
    for (const entity of feed.entity) {
      const rows = await db.all(sqlStatement, [entity.id, entity.vehicle?.vehicle?.id, entity.vehicle?.trip?.tripId, entity.vehicle?.trip?.routeId]);
      counterEntities++;
      if (rows.length === 1) {
        counterMatches++;
      }
    }
    return [counterEntities, countRows, counterMatches];
  }
  public async validateTripUpdates(pathToRawFile: string, pathToDB: string): Promise<[number, number, number]> {
    const sqlStatement = `SELECT * FROM rt_trip_update WHERE [entity.id] = ? AND [entity.trip_update.trip.trip_id] = ? AND [entity.trip_update.trip.route_id] = ? AND [entity.trip_update.stop_time_update.stop_sequence] = ?`;
    const db = await this.initDatabase(pathToDB);

    const feed = this.initFeedMessage(pathToRawFile);
    let counterEntities = 0;
    const countRows = (await db.get(`SELECT  COUNT(*) count FROM rt_trip_update`)).count;
    let counterMatches = 0;

    for (const entity of feed.entity) {
      for (const stopTimeUpdate of entity.tripUpdate?.stopTimeUpdate as GtfsRealtimeBindings.transit_realtime.TripUpdate.IStopTimeUpdate[]) {
        const rows = await db.all(sqlStatement, [entity.id, entity.tripUpdate?.trip.tripId, entity.tripUpdate?.trip.routeId, stopTimeUpdate.stopSequence?.toString()]);
        counterEntities++;
        if (rows.length === 1) {
          counterMatches++;
        }
      }
    }
    return [counterEntities, countRows, counterMatches];
  }
  private initFeedMessage(path: string) {
    const file = fs.readFileSync(path);
    return GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(new Uint8Array(file));
  }

  private async initDatabase(path: string) {
    return open({
      filename: path,
      driver: sqlite3.Database,
    });
  }

  private async readCSVFile(filePath: string): Promise<object[]> {
    const results: object[] = [];

    return new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (data) => results.push(data))
        .on("end", () => resolve(results))
        .on("error", (error) => reject(error));
    });
  }

  async validateGtfsRT() {
    console.log("\n\n");
    console.log("------------------started validation of GTFS-RT------------------");
    const tripUpdateResult = await this.validateTripUpdates("resources/brest-metropole-gtfs-rt/bibus-brest-gtfs-rt-trip-update", PATH_PIPELINE_DB);
    const all_rows_are_matching_tripUpdateResult = tripUpdateResult[2] === tripUpdateResult[1] && tripUpdateResult[2] === tripUpdateResult[0];

    const alertResult = await this.validateAlerts("resources/brest-metropole-gtfs-rt/bibus-brest-gtfs-rt-alerts", PATH_PIPELINE_DB);
    const all_rows_are_matching_alertResult = alertResult[2] === alertResult[1] && alertResult[2] === alertResult[0];

    const vehiclePositionsResult = await this.validateVehiclePositions("resources/brest-metropole-gtfs-rt/bibus-brest-gtfs-rt-vehicle-position", PATH_PIPELINE_DB);
    const all_rows_are_matching_vehiclePositionsResult = vehiclePositionsResult[2] === vehiclePositionsResult[1] && vehiclePositionsResult[2] === vehiclePositionsResult[0];

    const table = [
      { entity: "trip_update", "#rows_src": tripUpdateResult[0], "#rows_sink": tripUpdateResult[0], valid: all_rows_are_matching_tripUpdateResult },
      { entity: "alert", "#rows_src": alertResult[0], "#rows_sink": alertResult[0], valid: all_rows_are_matching_alertResult },
      { entity: "vehicle", "#rows_src": vehiclePositionsResult[0], "#rows_sink": vehiclePositionsResult[0], valid: all_rows_are_matching_vehiclePositionsResult },
    ];
    console.table(table);
    console.log("------------------finished validation of GTFS-RT-----------------");
    console.log("\n\n");
  }

  async validateGtfs() {
    console.log("\n\n");
    console.log("------------------started validation of GTFS------------------");
    const db = await this.initDatabase(PATH_PIPELINE_DB);
    const files = await fs.promises.readdir("resources/brest-metropole-gtfs/gtfs");
    const table = [];
    for (const filename of files) {
      const file = await this.readCSVFile("resources/brest-metropole-gtfs/gtfs/" + filename);
      const file_name_no_extension = filename.split(".")[0];
      const sqlStatement = "SELECT * FROM static_" + file_name_no_extension;
      const rows = await db.all(sqlStatement);
      const all_rows_are_matching = JSON.stringify(file) === JSON.stringify(rows);
      const outputRow = { dimension: file_name_no_extension, "#rows_src": file.length, "#rows_sink": rows.length, valid: all_rows_are_matching };
      table.push(outputRow);
    }
    console.table(table);
    console.log("------------------finished validation of GTFS------------------");
    console.log("\n\n");
  }

  printTable() {
    const table = [
      { entity: "TripUpdate", "#rows_manual_import": 1, "#rows_sink": 2, valid: true },
      { entity: "TripUpdate", "#rows_manual_import": 1, "#rows_sink": 2, valid: true },
      { entity: "TripUpdate", "#rows_manual_import": 1, "#rows_sink": 2, valid: true },
    ];
    console.table(table, ["entity", "#rows_manual_import", "#rows_sink", "valid"]);
  }

  async archiveGtfsAndGtfsRT(duration: number, interval: number) {
    //const command = "jv gtfs-static-and-rt.jv";
    const command = "echo hello world";
    const startTime = new Date().getTime();

    const intervalId = setInterval(() => {
      // Code to be executed at each interval
      const currentTime = new Date().getTime(); // Get the current time in milliseconds
      if (currentTime - startTime >= duration) {
        clearInterval(intervalId); // Stop the interval when the duration has elapsed
      }

      exec(command, (error, stdout, stderr) => {
        if (error) {
          console.error(`Error executing command: ${error}`);
        }
        console.log(`Standard output: ${stdout}`);
        console.log(`Standard error: ${stderr}`);
        if (!fs.existsSync("logs")) {
          fs.mkdirSync("logs");
        }
        fs.writeFileSync("logs/gtfsstaticandrt-" + startTime + "-" + currentTime + ".txt", stdout);
        const formatter = new Intl.DateTimeFormat("de-DE", {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          hour12: false,
        });

        const fileSize = fs.statSync("resources/gtfs-static-and-rt_pipeline-result.sqlite").size;
        fs.appendFileSync("logs/gtfsstaticandrt-sizebytime-" + startTime + ".txt", formatter.format(currentTime).replace(" ", "") + "," + fileSize + "\n");
      });
    }, interval); // Set the interval duration
    // Use setTimeout to stop the interval after the duration has elapsed even if the interval is still running
    setTimeout(() => {
      clearInterval(intervalId);
    }, duration);
  }
}
