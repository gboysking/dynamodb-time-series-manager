import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { DescribeTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { TimeSeriesStatisticsManager } from ".";

describe("TimeSeriesStatisticsManager", () => {
    let manager: TimeSeriesStatisticsManager;
    const testTable = "test-table";
    const testClient = new DynamoDBClient({});
    const testTimePartitions = [
        {
            name: "minute",
            format: "YYYY-MM-DDTHH:mm:00.000Z",
            interval: 60,
        },
        {
            name: "hour",
            format: "YYYY-MM-DDTHH:00:00.000Z",
            interval: 60 * 60,
        },
    ];

    beforeAll(async () => {
        manager = new TimeSeriesStatisticsManager({
            table: testTable,
            client: DynamoDBDocument.from(testClient),
            timePartitions: testTimePartitions,
        });

        await manager.onReady();
    });

    afterAll(async () => {
        await testClient.send(new DescribeTableCommand({ TableName: testTable }));
    });

    describe("addStatistic", () => {
        it("adds a statistic to the DynamoDB table for each time partition", async () => {
            const topic = "test-topic";
            const timestamp = Date.now() / 1000;

            await manager.addStatistic(topic, timestamp);

            const results = await Promise.all(testTimePartitions.map(async (partition) => {
                const startTime = Math.floor(timestamp / partition.interval) * partition.interval;
                const endTime = startTime + partition.interval - 1;

                return manager.getStatisticsPeriod(topic, partition.name, startTime, endTime);
            }));

            expect(results.flat().length).toBe(testTimePartitions.length);
        });
    });

    describe("getStatisticsPeriod", () => {
        it("returns the statistics for the specified time range", async () => {
            const topic = "test-topic";
            const timestamp = Date.now() / 1000;

            await manager.addStatistic(topic, timestamp);

            const partition = testTimePartitions[0];
            const startTime = Math.floor(timestamp / partition.interval) * partition.interval;
            const endTime = startTime + partition.interval - 1;

            const result = await manager.getStatisticsPeriod(topic, partition.name, startTime, endTime);

            expect(result).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({
                        topic,
                        period: partition.name,
                        count: 2,
                        time_partition: startTime,
                    }),
                ])
            );
        });
    });

    describe("getStatistics", () => {
        it("returns the statistics for all time partitions for the specified time range", async () => {
            const topic = "test-topic";
            const timestamp = Date.now() / 1000;

            await manager.addStatistic(topic, timestamp);

            const result = await manager.getStatistics(topic, timestamp - 3600, timestamp);

            expect(result).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({
                        topic,
                        count: 3,
                    }),
                ])
            );
        });
    });
});
