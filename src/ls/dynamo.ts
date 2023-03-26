import * as dynamodb from '@aws-sdk/client-dynamodb';
import {  DynamoDBClient,  DynamoDBClientConfig  } from '@aws-sdk/client-dynamodb';
const attr = require('dynamodb-data-types').AttributeValue;

export interface DynamoDBConfig extends DynamoDBClientConfig {
  maxRows: number;
}

const dynamoDBScalarTypesMapper = {
  'N': 'number',
  'S': 'string',
  'B': 'buffer',
  'BOOL': 'boolean',
  'SS': 'stringset',
  'NS': 'numberset',
  'BS': 'binaryset',
  'M': 'map',
  'L': 'list',
};

enum Ordered {
  LOW = 0,
  MEDIUM,
  HIGH,
}

export interface Column {
  columnName: string
  columnType: string
  keyType?: string
  sortOrdered: Ordered
}

export interface Index {
  indexName: string
  keys: Column[]
}

export interface Table {
  tableName: string
  indexes: Index[]
  columns: Column[]
  rawData: any
}

export class DynamoDBLib {
  private client: DynamoDBClient;

  private maxRows: number;

  private readonly limitRegexPattern = /limit\s+\d+/gi;

  constructor(config: DynamoDBConfig) {
    this.client = new DynamoDBClient(config);
    this.maxRows = config.maxRows;
  }

  public async listTables(): Promise<string[]> {
    let exclusiveStart: string;
    const tablesNames: string[] = [];
    while (true) {
      const command = new dynamodb.ListTablesCommand({
        ExclusiveStartTableName: exclusiveStart,
      });
      const result = await this.client.send(command);
      tablesNames.push(...result.TableNames);  
      exclusiveStart = result.LastEvaluatedTableName;
      if (!exclusiveStart) {
        break;
      }
    }
    return tablesNames;
  }

  private async getColumns(tableName: string): Promise<Column[]> {
    const command = new dynamodb.ScanCommand({
      TableName: tableName,
      Limit: 1,
    });
    const result = await this.client.send(command);
    const item = result.Items[0] || {};

    const cols = Object.keys(item).map(key => {
      const val = item[key];
      const dataType = Object.keys(dynamoDBScalarTypesMapper).find(type => !!val[type]) || '';
      return <Column>{
        columnName: key,
        columnType: dynamoDBScalarTypesMapper[dataType] || 'unknown',
        sortOrdered: Ordered.LOW,
      };
    });
    return cols;
  }

  public async describeTable(tableName: string): Promise<Table> {
    const command = new dynamodb.DescribeTableCommand({ 
      TableName: tableName, 
    });
    const result = await this.client.send(command);
    const cols = await this.getColumns(tableName);
    const indexes = result.Table?.GlobalSecondaryIndexes?.map(gsi => <Index>{
      indexName: gsi.IndexName,
      keys: gsi.KeySchema.map(key => <Column>{
        columnName: key.AttributeName,
        keyType: key.KeyType,
      }),
    }) || [];
    
    for (const key of result.Table.KeySchema) {
      const col = cols.find(x => x.columnName === key.AttributeName);
      if (col) {
        col.keyType = key.KeyType;
        col.sortOrdered = col.keyType === 'HASH' 
          ? Ordered.HIGH  
          : Ordered.MEDIUM;
      }
    }
    cols.sort((left, right) => right.sortOrdered - left.sortOrdered);

    return <Table>{
      tableName: tableName,
      columns: cols,
      indexes,
      rawData: result,
    };
  }

  private findLimit(query: string): number | undefined { 
    const matchedTexts = query.match(this.limitRegexPattern);
    if (!matchedTexts) {
      return undefined;
    }
    const limitPattern = matchedTexts[0];
    const words = limitPattern.split(/\s+/);
    const number = words[1];
    return parseInt(number, 10);
  }

  private removeLimitKeyword(query: string): string {
    return query.replace(/\s+/g, ' ').replace(this.limitRegexPattern, '');
  }

  private async queryWithOptions(query: string, opts: { limit?: number } = {}): Promise<any[]> {
    let next;
    let items = [];
    do {
      const command = new dynamodb.ExecuteStatementCommand({
        Statement: query,
        ConsistentRead: false,
        ReturnConsumedCapacity: 'NONE',
        Limit: this.maxRows,
        NextToken: next,
      });
      const result = await this.client.send(command);
      next = result.NextToken;
      items.push(...result.Items);
      if (opts.limit && items.length >= opts.limit) {
        break;
      }
    } while (next);
    return opts.limit ? items.splice(0, opts.limit) : items;
  }

  public async query(query: string, opts: { limit?: number } = {})  {
    const limit = this.findLimit(query) || opts.limit;
    const rawItems = await this.queryWithOptions(this.removeLimitKeyword(query), { limit });

    let cols = {};
    const items = [];
    for (let i = 0; i < rawItems.length; i++) {
      const rawItem = rawItems[i];
      cols = {
        ...cols,
        ...Object.keys(rawItem).reduce((prev, x) => ({ ...prev, [x]: 1 }), {}),
      };
      items.push(attr.unwrap(rawItem));
    }
    return {
      cols: Object.keys(cols),
      items,
    };
  }
}


