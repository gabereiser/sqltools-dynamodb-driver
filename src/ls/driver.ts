import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { DynamoDBLib, DynamoDBConfig } from './dynamo';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
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
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          { 
            label: 'Tables', 
            type: ContextValue.RESOURCE_GROUP, 
            iconId: 'folder', 
            childType: ContextValue.TABLE, 
          },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return <MConnectionExplorer.IChildItem[]>[
          { 
            label: 'Schema', 
            type: ContextValue.RESOURCE_GROUP, 
            iconId: 'layers', 
            childType: ContextValue.COLUMN, 
          },
          { 
            label: 'Indexes', 
            type: ContextValue.RESOURCE_GROUP, 
            iconId: 'library', 
            childType: ContextValue.SCHEMA, 
          },
        ];
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
    switch (item.childType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        const tableNames = await this.listTables();
        return tableNames.map(table => <MConnectionExplorer.IChildItem>{
          label: table,
          type: ContextValue.TABLE,
          iconId: 'table',
          childType: ContextValue.COLUMN,
          schema: item.schema,
          database: item.database,
        });
      case ContextValue.COLUMN:
      case ContextValue.SCHEMA: 
        const tableName = parent.label;
        const api = await this.open();
        const table = await api.describeTable(tableName);

        if (item.childType === ContextValue.COLUMN) {
          return table.columns.map(col => <NSDatabase.IColumn>{
            label: `${col.columnName}`,
            type: ContextValue.COLUMN,
            dataType: col.columnType,
            childType: ContextValue.NO_CHILD,
            iconName: col.keyType === 'HASH' ? 'pk' 
              : (col.keyType === 'RANGE' ? 'fk' 
                : 'column'
              ), 
            isNullable: !col.keyType,
            schema: item.schema,
            database: item.database,
            extra: {
              'keyType': col.keyType,
            },
            table: item,
          });
        }

        return table.indexes.map(indexName => <MConnectionExplorer.IChildItem>{
          label: `${indexName}`,
          type: ContextValue.NO_CHILD,
          childType: ContextValue.NO_CHILD,
          iconId: 'package',
          schema: item.schema,
          database: item.database,
        });
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

    if (tables.length < 5) {
      const queryResult = await Promise.all(
        tables.map(async table => {
          return api.describeTable(table).then(data => {
            return data.indexes.map(indexName => <NSDatabase.SearchableItem>{
              type: ContextValue.TABLE,
              label:`"${table}"."${indexName}"`,
              childType: ContextValue.COLUMN,
            });
          });
        }),
      );
      for (const items of queryResult) {
        result.push(...items);
      }
    }

    return result;
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  };
}
