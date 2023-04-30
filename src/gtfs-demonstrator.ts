import * as fs from "fs";
import * as GtfsRealtimeBindings from "gtfs-realtime-bindings";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

const PATH_PIPELINE_DB = "resources/gtfs-static-and-rt_pipeline-result.sqlite";
const PATH_MANUAL_DB = "gtfs-static_manual-import";

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

  async validateGtfsRT() {
    console.log("------------------started------------------");
    const tripUpdateResult = await this.validateTripUpdates("resources/brest-metropole-gtfs-rt/bibus-brest-gtfs-rt-trip-update", PATH_PIPELINE_DB);
    const alertResult = await this.validateAlerts("resources/brest-metropole-gtfs-rt/bibus-brest-gtfs-rt-alerts", PATH_PIPELINE_DB);
    const vehiclePositionsResult = await this.validateVehiclePositions("resources/brest-metropole-gtfs-rt/bibus-brest-gtfs-rt-vehicle-position", PATH_PIPELINE_DB);
    console.log("TripUpdate --> #rows in manual import: " + tripUpdateResult[0] + ", #rows in processed table:  " + tripUpdateResult[1] + " matches found: " + tripUpdateResult[2] + " --> " + (tripUpdateResult[2] === tripUpdateResult[1] && tripUpdateResult[2] === tripUpdateResult[0] ? "Validation valid ✅" : "Validation not valid ❌"));
    console.log("Alert --> #rows in manual import: " + alertResult[0] + ", #rows in processed table:  " + alertResult[1] + " matches found: " + alertResult[2] + " --> " + (tripUpdateResult[2] === tripUpdateResult[1] && tripUpdateResult[2] === tripUpdateResult[0] ? " Validation successfull ✅" : " Validation not valid ❌"));
    console.log("VehiclePosition --> #rows in manual import:  " + vehiclePositionsResult[0] + ", #rows in processed table:  " + vehiclePositionsResult[1] + " matches found: " + vehiclePositionsResult[2] + " --> " + (tripUpdateResult[2] === tripUpdateResult[1] && tripUpdateResult[2] === tripUpdateResult[0] ? "Validation successfull ✅" : "Validation not valid ❌"));
    console.log("------------------finished------------------");
  }

  validateGtfs() {
    console.log("------------------started------------------");
    const sqlStatement = `
    SELECT * FROM <manual_imported_table>
    EXCEPT
    SELECT * FROM <pipeline_created_table>
    UNION ALL
    SELECT * FROM <pipeline_created_table>
    EXCEPT
    SELECT * FROM <manual_imported_table> 
    `;

    console.log("------------------finished------------------");
  }
}
