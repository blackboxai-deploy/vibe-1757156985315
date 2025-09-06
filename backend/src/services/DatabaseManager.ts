import knex, { Knex } from 'knex';
import path from 'path';
import { Logger } from './Logger';

export class DatabaseManager {
  private db: Knex;
  private logger: Logger;

  constructor() {
    this.logger = new Logger();
    this.initializeDatabase();
  }

  private initializeDatabase() {
    const dbConfig: Knex.Config = {
      client: process.env.DATABASE_TYPE === 'postgresql' ? 'pg' : 'sqlite3',
      connection: this.getConnectionConfig(),
      migrations: {
        directory: path.join(__dirname, '../database/migrations'),
        tableName: 'knex_migrations'
      },
      seeds: {
        directory: path.join(__dirname, '../database/seeds')
      },
      useNullAsDefault: true,
      // SQLite specific settings
      ...(process.env.DATABASE_TYPE !== 'postgresql' && {
        connection: {
          filename: process.env.DATABASE_PATH || path.join(__dirname, '../../data/messaging_suite.db')
        },
        pool: {
          afterCreate: (conn: any, cb: any) => {
            conn.run('PRAGMA foreign_keys = ON', cb);
          }
        }
      })
    };

    this.db = knex(dbConfig);
  }

  private getConnectionConfig() {
    if (process.env.DATABASE_TYPE === 'postgresql') {
      return {
        host: process.env.DB_HOST || 'localhost',
        port: parseInt(process.env.DB_PORT || '5432'),
        user: process.env.DB_USER || 'messaging_suite',
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME || 'messaging_suite',
        ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
      };
    }
    
    // SQLite
    return {
      filename: process.env.DATABASE_PATH || path.join(__dirname, '../../data/messaging_suite.db')
    };
  }

  async initialize(): Promise<void> {
    try {
      // Test connection
      await this.db.raw('SELECT 1');
      this.logger.info('Database connection established');
      
      // Run migrations
      await this.runMigrations();
      
      this.logger.info('Database initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize database:', error);
      throw error;
    }
  }

  private async runMigrations(): Promise<void> {
    try {
      const [batchNo, log] = await this.db.migrate.latest();
      
      if (log.length === 0) {
        this.logger.info('Database is up to date');
      } else {
        this.logger.info(`Ran ${log.length} migrations`, { batch: batchNo, migrations: log });
      }
    } catch (error) {
      this.logger.error('Migration failed:', error);
      throw error;
    }
  }

  async close(): Promise<void> {
    try {
      await this.db.destroy();
      this.logger.info('Database connection closed');
    } catch (error) {
      this.logger.error('Failed to close database connection:', error);
      throw error;
    }
  }

  // Getter for database instance
  get instance(): Knex {
    return this.db;
  }

  // Transaction helper
  async transaction<T>(callback: (trx: Knex.Transaction) => Promise<T>): Promise<T> {
    return this.db.transaction(callback);
  }

  // Health check
  async healthCheck(): Promise<{ status: 'ok' | 'error', latency: number, error?: string }> {
    const startTime = Date.now();
    
    try {
      await this.db.raw('SELECT 1');
      const latency = Date.now() - startTime;
      return { status: 'ok', latency };
    } catch (error) {
      const latency = Date.now() - startTime;
      return { 
        status: 'error', 
        latency, 
        error: error instanceof Error ? error.message : 'Unknown database error' 
      };
    }
  }

  // Query builder helpers
  contacts() {
    return this.db('contacts');
  }

  accounts() {
    return this.db('accounts');
  }

  devices() {
    return this.db('devices');
  }

  campaigns() {
    return this.db('campaigns');
  }

  messages() {
    return this.db('messages');
  }

  templates() {
    return this.db('templates');
  }

  suppressions() {
    return this.db('suppressions');
  }

  auditLogs() {
    return this.db('audit_logs');
  }

  users() {
    return this.db('users');
  }

  // Batch operations for performance
  async batchInsert(table: string, data: any[], chunkSize: number = 100): Promise<void> {
    const chunks = this.chunkArray(data, chunkSize);
    
    for (const chunk of chunks) {
      await this.db(table).insert(chunk);
    }
  }

  async batchUpdate(table: string, data: any[], identifier: string, chunkSize: number = 100): Promise<void> {
    const chunks = this.chunkArray(data, chunkSize);
    
    await this.transaction(async (trx) => {
      for (const chunk of chunks) {
        const promises = chunk.map((item: any) => 
          trx(table).where(identifier, item[identifier]).update(item)
        );
        await Promise.all(promises);
      }
    });
  }

  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  // Search helpers
  async searchContacts(query: string, limit: number = 50): Promise<any[]> {
    return this.db('contacts')
      .where('name', 'like', `%${query}%`)
      .orWhere('email', 'like', `%${query}%`)
      .orWhere('phone', 'like', `%${query}%`)
      .limit(limit);
  }

  async searchCampaigns(query: string, limit: number = 50): Promise<any[]> {
    return this.db('campaigns')
      .where('name', 'like', `%${query}%`)
      .orWhere('description', 'like', `%${query}%`)
      .limit(limit);
  }

  // Statistics helpers
  async getContactStats(): Promise<any> {
    const [total, active, suppressed] = await Promise.all([
      this.db('contacts').count('* as count').first(),
      this.db('contacts').where('opt_in_timestamp', 'is not', null).count('* as count').first(),
      this.db('suppressions').count('* as count').first()
    ]);

    return {
      total: total?.count || 0,
      active: active?.count || 0,
      suppressed: suppressed?.count || 0
    };
  }

  async getCampaignStats(): Promise<any> {
    const stats = await this.db('campaigns')
      .select('status')
      .count('* as count')
      .groupBy('status');

    return stats.reduce((acc: any, stat: any) => {
      acc[stat.status] = stat.count;
      return acc;
    }, {});
  }

  async getMessageStats(timeframe: 'today' | '7days' | '30days' = '7days'): Promise<any> {
    const now = new Date();
    let startDate: Date;

    switch (timeframe) {
      case 'today':
        startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        break;
      case '7days':
        startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        break;
      case '30days':
        startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
        break;
    }

    const stats = await this.db('messages')
      .select('status', 'channel')
      .count('* as count')
      .where('created_at', '>=', startDate)
      .groupBy(['status', 'channel']);

    return stats;
  }
}