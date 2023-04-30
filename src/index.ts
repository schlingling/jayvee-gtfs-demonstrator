import { GtfsDemonstrator } from "./gtfs-demonstrator";

const validator = new GtfsDemonstrator();
validator.validateGtfs().then(() => validator.validateGtfsRT());
