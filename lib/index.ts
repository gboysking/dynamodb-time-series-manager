import { DynamoDBDocument, QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { CreateTableCommand, DescribeTableCommand, DescribeTableCommandInput, DynamoDBClient, TableStatus } from "@aws-sdk/client-dynamodb";
import moment from 'moment';

interface Statistic {
    topic: string;
    period: string;
    count: number;
    time_partition: number;
}

interface TimePartition {
    name: string;
    format: string;
    interval: number;
}

const timePartitions: TimePartition[] = [
    {
        name: "minute",
        format: "YYYY-MM-DDTHH:mm:00.000Z",
        interval: 60
    },
    {
        name: "hour",
        format: "YYYY-MM-DDTHH:00:00.000Z",
        interval: 60 * 60
    },
    {
        name: "day",
        format: "YYYY-MM-DDT00:00:00.000Z",
        interval: 60 * 60 * 24
    },
    {
        name: "month",
        format: "YYYY-MM-01T00:00:00.000Z",
        interval: 60 * 60 * 24 * 30
    },
    {
        name: "year",
        format: "YYYY-01-01T00:00:00.000Z",
        interval: 60 * 60 * 24 * 365
    }
];

interface TimeSeriesStatisticsManagerOptions {
    table?: string;
    client?: DynamoDBDocument;
    timePartitions?: TimePartition[];
}

export class TimeSeriesStatisticsManager {
    private client: DynamoDBDocument;
    private table: string;
    private state: 'INITIALIZING' | 'INITIALIZED' | "FAIL";
    private onReadyPromises: Array<(value?: unknown) => void>;
    private timePartitions: TimePartition[];

    constructor(options: TimeSeriesStatisticsManagerOptions) {
        if (options.client) {
            this.client = DynamoDBDocument.from(options.client);
        } else {
            this.client = DynamoDBDocument.from(new DynamoDBClient({}));
        }

        if (options.timePartitions) {
            this.timePartitions = [...options.timePartitions];
        } else {
            this.timePartitions = [...timePartitions];
        }

        if (options.table) {
            this.table = options.table;
        } else {
            this.table = "statistics";
        }

        this.state = 'INITIALIZING';
        this.onReadyPromises = [];

        Promise.resolve()
            .then(() => {
                return this.createTableIfNotExists();
            })
            .then(() => {
                this.state = 'INITIALIZED';
                this.resolveReadyPromises();
            })
            .catch((error) => {
                this.state = "FAIL";
                this.rejectReadyPromises(error);
            });
    }

    onReady(): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.state === 'INITIALIZED') {
                resolve();
            } else if (this.state === 'FAIL') {
                reject();
            } else {
                this.onReadyPromises.push((error) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve();
                    }
                });                
            }
        });
    }

    private resolveReadyPromises(): void {
        for (const resolve of this.onReadyPromises) {
            resolve();
        }
        this.onReadyPromises = [];
    }

    private rejectReadyPromises(error: any): void {
        for (const resolve of this.onReadyPromises) {
            resolve(error);
        }
        this.onReadyPromises = [];
    }

    // Wait until the table exists
    async waitUntilTableExists(timeout: number = 6000): Promise<void> {
        const command: DescribeTableCommandInput = { TableName: this.table };
        const startTime = Date.now();
        const endTime = startTime + timeout;

        while (Date.now() < endTime) {
            try {
                let result = await this.client.send(new DescribeTableCommand(command));

                if (result.Table.TableStatus == TableStatus.ACTIVE) {
                    return;
                } else if (result.Table.TableStatus == TableStatus.DELETING || result.Table.TableStatus == TableStatus.INACCESSIBLE_ENCRYPTION_CREDENTIALS) {
                    break;
                }

                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (e) {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }

        throw new Error(`Timed out waiting for table ${this.table} to exist`);
    }

    async createTableIfNotExists(): Promise<void> {
        try {
            await this.client.send(new DescribeTableCommand({ TableName: this.table }));
        } catch (error: any) {
            if (error.name === "ResourceNotFoundException") {
                const params = {
                    AttributeDefinitions: [
                        { AttributeName: "topic_period", AttributeType: "S" },
                        { AttributeName: "time_partition", AttributeType: "N" }
                    ],
                    KeySchema: [
                        { AttributeName: "topic_period", KeyType: "HASH" },
                        { AttributeName: "time_partition", KeyType: "RANGE" }
                    ],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 5,
                        WriteCapacityUnits: 5
                    },
                    TableName: this.table
                };

                await this.client.send(new CreateTableCommand(params));

                // Wait until table is active
                await this.waitUntilTableExists();
            } else {
                console.error(
                    "Error checking for the existence of the DynamoDB table:",
                    error
                );
                throw error;
            }
        }
    }

    private createTopicPeriod(topic: string, period: string,): string {
        return `${topic}#${period}`;
    }

    private getTimePartition(period: string): TimePartition {
        return this.timePartitions.find((tp) => tp.name === period);
    }

    private getTimePartitionValue(timestamp: number, partition: TimePartition): string {
        const date = moment(timestamp);
        const formattedDate = date.utc().format(partition.format)
        return formattedDate;
    }

    public async addStatistic(topic: string, timestamp: number, amount?: number): Promise<void> {
        await this.onReady();

        for (const partition of this.timePartitions) {
            const timePartitionValue = this.getTimePartitionValue(timestamp, partition);
            const timePartition = new Date(timePartitionValue);

            const updateParams = {
                TableName: this.table,
                Key: {
                    topic_period: this.createTopicPeriod(topic, partition.name),
                    time_partition: timePartition.getTime()
                },
                UpdateExpression: "ADD #count :incr SET #timestamp = :time",
                ExpressionAttributeNames: {
                    "#count": "count",
                    "#timestamp": "time"
                },
                ExpressionAttributeValues: {
                    ":incr": amount ? amount : 1,
                    ":time": timePartition.toISOString()
                },
                ReturnValues: "UPDATED_NEW"
            };

            try {
                const updateCommand = new UpdateCommand(updateParams);
                await this.client.send(updateCommand);
            } catch (error) {
                console.error(`Error updating ${partition.name} statistic:`, JSON.stringify(error, null, 2));
                throw error;
            }
        }

    }

    public async getStatisticsPeriod(topic: string, period: string, startTime: number, endTime: number): Promise<Statistic[]> {
        await this.onReady();

        const periodTopic = this.createTopicPeriod(topic, period);
        const partition = this.getTimePartition(period);
        startTime = Math.floor(startTime / partition.interval) * partition.interval;
        endTime = Math.floor(endTime / partition.interval) * partition.interval;

        const queryParams = {
            TableName: this.table,
            KeyConditionExpression: "topic_period = :topic_period  AND time_partition BETWEEN :startTime AND :endTime",
            ExpressionAttributeValues: {
                ":topic_period": periodTopic,
                ":startTime": startTime,
                ":endTime": endTime
            }
        };

        try {
            const response = await this.client.send(new QueryCommand(queryParams));
            if (response.Items) {
                return response.Items.map((item) => {
                    const [topic, period] = item.topic_period.split("#");
                    return {
                        topic,
                        period,
                        count: item.count,
                        time_partition: item.time_partition,
                    };
                }) as Statistic[];
            } else {
                return [];
            }
        } catch (error) {
            console.error("Error getting statistic:", JSON.stringify(error, null, 2));
            throw error;
        }
    }

    public async getStatistics(topic: string, startTime: number, endTime: number): Promise<Statistic[]> {
        await this.onReady();

        const results = await Promise.all(this.timePartitions.map(async (partition) => {
            const result = await this.getStatisticsPeriod(topic, partition.name, startTime, endTime);
            return result;
        }));

        return results.flat();
    }
}
