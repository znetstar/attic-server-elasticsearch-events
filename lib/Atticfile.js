"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AtticServerElasticSearchEvents = void 0;
const elasticsearch_1 = require("@elastic/elasticsearch");
const to_pojo_1 = require("@thirdact/to-pojo");
class AtticServerElasticSearchEvents {
    constructor(applicationContext, eventsIndexPrefix = applicationContext.config.eventsIndexPrefix) {
        this.applicationContext = applicationContext;
        this.eventsIndexPrefix = eventsIndexPrefix;
        this.client = new elasticsearch_1.Client({ node: this.config.eventsElasticSearchUri || process.env.EVENTS_ELASTICSEARCH_URI });
    }
    get config() { return this.applicationContext.config; }
    async init() {
        const ctx = this.applicationContext;
        for (const event of this.config.sendEventsToElasticSearch || []) {
            ctx.registerHook(`events.${event}`, async (event) => {
                const toPojo = new to_pojo_1.ToPojo();
                const body = {
                    ...(toPojo.toPojo(event, {
                        ...toPojo.DEFAULT_TO_POJO_OPTIONS,
                        conversions: [
                            ...to_pojo_1.makeBinaryEncoders('base64'),
                            ...(toPojo.DEFAULT_TO_POJO_OPTIONS.conversions || [])
                        ]
                    }))
                };
                debugger;
                this.client.index({
                    index: `${this.eventsIndexPrefix ? this.eventsIndexPrefix + '.' : ''}${event}`,
                    id: event._id,
                    body
                });
            });
        }
    }
    get name() {
        return '@znetstar/attic-server-elasticsearch-events';
    }
}
exports.AtticServerElasticSearchEvents = AtticServerElasticSearchEvents;
exports.default = AtticServerElasticSearchEvents;
//# sourceMappingURL=Atticfile.js.map