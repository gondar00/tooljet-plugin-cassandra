const cassandra = require('cassandra-driver');

export class QueryError extends Error {
  data: Record<string, unknown>;
  description: any;
  constructor(message: string | undefined, description: any, data: Record<string, unknown>) {
    super(message);
    this.name = this.constructor.name;
    this.data = data;
    this.description = description;

    console.log(this.description);
  }
}

export type QueryResult = {
  status: 'ok' | 'failed' | 'needs_oauth';
  errorMessage?: string;
  data: Array<object> | object;
};

export interface QueryService {
  run(
    sourceOptions: object,
    queryOptions: object,
    dataSourceId?: string,
    dataSourceUpdatedAt?: string
  ): Promise<QueryResult>;
  getConnection?(queryOptions: object, options: any, checkCache: boolean, dataSourceId: string): Promise<object>;
  testConnection?(sourceOptions: object): Promise<ConnectionTestResult>;
}

export type ConnectionTestResult = {
  status: 'ok' | 'failed';
  message?: string;
  data?: object;
};

export default class RedisQueryService implements QueryService {
  async run(sourceOptions: any, queryOptions: any): Promise<any> {
    let result = {};
    const query = queryOptions.query;

    const client = await this.getConnection(sourceOptions);

    try {
      result = await client.execute(query);
    } catch (err) {
      client.disconnect();
      throw new QueryError('Query could not be completed', err.message, {});
    }

    return { status: 'ok', data: result };
  }

  async testConnection(sourceOptions: any): Promise<any> {
    const client = await this.getConnection(sourceOptions);
    await client.execute('');

    return {
      status: 'ok',
    };
  }

  async getConnection(sourceOptions: any): Promise<any> {
    const contactPoints = sourceOptions.contactPoints.split(',');
    const localDataCenter = sourceOptions.localDataCenter;
    const keyspace = sourceOptions.keyspace;

    const client = new cassandra.Client({
      contactPoints,
      localDataCenter,
      keyspace
    });

    return client;
  }
}