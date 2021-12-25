import { IApplicationContext, IConfig, IPlugin } from "@znetstar/attic-common/lib/Server";
import { Client } from '@elastic/elasticsearch';
export declare type AtticElasticSearchEventsConfig = IConfig & {
    eventsElasticSearchUri?: string;
    sendEventsToElasticSearch?: string[];
    eventsIndexPrefix?: string;
};
export declare class AtticServerElasticSearchEvents implements IPlugin {
    applicationContext: IApplicationContext;
    eventsIndexPrefix: string;
    client: Client;
    constructor(applicationContext: IApplicationContext, eventsIndexPrefix?: string);
    get config(): AtticElasticSearchEventsConfig;
    init(): Promise<void>;
    get name(): string;
}
export default AtticServerElasticSearchEvents;
