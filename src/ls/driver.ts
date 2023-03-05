import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { DynamoDBLib, DynamoDBConfig } from './dynamo';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0, IQueryOptions } from '@sqltools/types';
import { v4 as generateId } from 'uuid';

export default class DynamoDbDriver 
  extends AbstractDriver<DynamoDBLib, DynamoDBConfig> 
  implements IConnectionDriver {

  queries = queries;

  public async open() {
    if (this.connection) {
      return this.connection;
    }
    
    const clientConfig: DynamoDBConfig = {
      region: this.credentials.region,
      credentials: {
        accessKeyId: this.credentials.accessKeyId,
        secretAccessKey: this.credentials.secretAccessKey,
      },
    };

    const client = new DynamoDBLib(clientConfig);
    this.connection = Promise.resolve(client);
    return  this.connection;
  }

  public async close() {
    if (!this.connection)  {
      return Promise.resolve();
    }
    
    this.connection = null;
  }

  public query: typeof AbstractDriver['prototype']['query'] = async (query, opt = {}) => {
    const { requestId } = opt;
    const messages = [];
    try {
      const api = await this.open();
      const { cols, items } = await api.query(query.toString());
      return [
        <NSDatabase.IResult>{
          requestId,
          connId: this.getId(),
          resultId: generateId(),
          cols,
          results: items,
          query: query,
          total: items.length,
          messages: messages.concat([
            this.prepareMessage([
              'Query successfully executed. 0 rows were affected.',
            ]),
          ]),
        },
      ];
    } catch (err) {
      return [
        <NSDatabase.IResult> {
          requestId,
          connId: this.getId(),
          resultId: generateId(),
          error: true,
          rawError: err,
          cols: [],
          results: [],
          query: query,
          messages: messages.concat([
            this.prepareMessage(
              [err.message.replace(/\n/g, ' ')].filter(Boolean).join(' '),
            ),
          ]),
        },
      ];
    }
  };

  public async testConnection() {
    await this.listTables();
  }

  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    // const api = await this.open();
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION: {
        const tableNames = await this.listTables();
        return tableNames.map(table => <MConnectionExplorer.IChildItem>{
          label: table,
          type: ContextValue.TABLE,
          iconId: 'table',
          childType: ContextValue.COLUMN,
          schema: 'table',
          database: item.database,
        });
      }
      case ContextValue.TABLE: {
        return <MConnectionExplorer.IChildItem[]>[
          {
            label: 'Schema',
            type: ContextValue.RESOURCE_GROUP,
            childType: ContextValue.COLUMN,
            iconId: 'package',
            schema: item.schema,
            database: item.database,
          }, {
            label: 'GIS',
            type: ContextValue.RESOURCE_GROUP,
            childType: ContextValue.TABLE,
            iconId: 'library',
            schema: item.schema,
            database: item.database,
          },
        ];
      } 
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  private async listTables(): Promise<string[]> {
    const api = await this.open();
    const tableNames = await api.listTables();
    const { filter } = this.credentials;
    if (!filter) {
      return tableNames;
    }
    const pattern = new RegExp(filter, 'i');
    return tableNames.filter(table => pattern.test(table));
  }

  private async getChildrenForGroup({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    const api = await this.open();
    const tableName = parent.label;
    const table = await api.describeTable(tableName);

    switch (item.childType) {
      case ContextValue.TABLE: {
        return table.indexes.map(index => <NSDatabase.ITable>{
          type: ContextValue.TABLE,
          label: `${index.indexName}`,
          childType: ContextValue.NO_CHILD,
          iconId: 'package',
        });
      }
      case ContextValue.COLUMN: {
        return table.columns.map(col => <NSDatabase.IColumn>{
          label: `${col.columnName}`,
          type: ContextValue.COLUMN,
          dataType: col.columnType,
          childType: ContextValue.NO_CHILD,
          iconName: col.keyType === 'HASH' ? 'pk' 
            : (col.keyType === 'RANGE' ? 'fk' 
              : 'column'
            ), 
          table: item,
        });
      }
    }
    
    return [];
  }

  private async searchTables(search: string): Promise<string[]> {
    const tables = await this.listTables();
    const pattern = new RegExp(search, 'i');
    return tables.filter(table => pattern.test(table));
  }

  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {   
    const result: NSDatabase.SearchableItem[] = [];
    if (itemType != ContextValue.TABLE) {
      return result;
    }

    const api = await this.open();
    const tables = await this.searchTables(search);
    for (const table of tables) {
      result.push(
        <NSDatabase.SearchableItem>{
          type: ContextValue.TABLE,
          label:`"${table}"`,
          childType: ContextValue.COLUMN,
        },
      );
    }

    const queryResult = await Promise.all(
      tables.map(async table => {
        return api.describeTable(table).then(data => {
          return data.indexes.map(index => <NSDatabase.SearchableItem>{
            type: ContextValue.TABLE,
            label:`"${table}"."${index.indexName}"`,
            childType: ContextValue.COLUMN,
          });
        });
      }),
    );
    for (const items of queryResult) {
      result.push(...items);
    }

    return result;
  }

  public async showRecords(table: NSDatabase.ITable, opt = {}): Promise<NSDatabase.IResult<any>[]> {
    const query = `SELECT * FROM "${table.label}"`;
    return this.query(query, opt);
  }

  public async describeTable(metadata: NSDatabase.ITable, opt: IQueryOptions): Promise<NSDatabase.IResult<any>[]> {
    const { requestId } = opt;
    const api = await this.open();
    const table = await api.describeTable(metadata.label);
    return <NSDatabase.IResult<any>[]>[{
      requestId,
      connId: this.getId(),
      resultId: generateId(),
      cols: ['Result'],
      results: [{
        result: table,
      }],
    }];
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  };
}
