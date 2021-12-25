import {IApplicationContext, IConfig, IPlugin} from "@znetstar/attic-common/lib/Server";
import {IEvent,ILocation, IRPC} from "@znetstar/attic-common";
import { Client } from '@elastic/elasticsearch';
import {
  ToPojo, makeBinaryEncoders
} from '@thirdact/to-pojo';

export type AtticElasticSearchEventsConfig = IConfig&{
  eventsElasticSearchUri?: string;
  sendEventsToElasticSearch?: string[];
  eventsIndexPrefix?: string;
};


export class AtticServerElasticSearchEvents implements IPlugin {
    public client: Client;
    constructor(
      public applicationContext: IApplicationContext,
      public eventsIndexPrefix: string = (applicationContext as any).config.eventsIndexPrefix
    ) {
      this.client = new Client({ node: this.config.eventsElasticSearchUri || process.env.EVENTS_ELASTICSEARCH_URI });
    }

    public get config(): AtticElasticSearchEventsConfig { return this.applicationContext.config as AtticElasticSearchEventsConfig; }

    public async init(): Promise<void> {
      const ctx = this.applicationContext;

      for (const event of this.config.sendEventsToElasticSearch || []) {
        ctx.registerHook(`events.${event}`, async (event: IEvent<unknown>) => {
          const toPojo = new ToPojo<any, any>();
          const body = {
            ...(toPojo.toPojo(event, {
              ...toPojo.DEFAULT_TO_POJO_OPTIONS,
              conversions: [
                ...makeBinaryEncoders('base64' as any),
                ...(toPojo.DEFAULT_TO_POJO_OPTIONS.conversions || [])
              ]
            }))
          };

          this.client.index({
            index: `${this.eventsIndexPrefix ? this.eventsIndexPrefix + '.' : ''}${body.type}`,
            id: body._id,
            body
          })
        });
      }
    }

    public get name(): string {
        return '@znetstar/attic-server-elasticsearch-events';
    }
}

export default AtticServerElasticSearchEvents;
