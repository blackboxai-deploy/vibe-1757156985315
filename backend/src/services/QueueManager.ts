import { Queue, Worker, Job, QueueEvents } from 'bullmq';
import IORedis from 'ioredis';
import { Logger } from './Logger';
import { DatabaseManager } from './DatabaseManager';

// Import job processors
import { EmailProcessor } from '../processors/EmailProcessor';
import { SMSProcessor } from '../processors/SMSProcessor';
import { WhatsAppProcessor } from '../processors/WhatsAppProcessor';
import { CampaignProcessor } from '../processors/CampaignProcessor';

export class QueueManager {
  private redis: IORedis;
  private logger: Logger;
  private dbManager: DatabaseManager;

  // Queues
  private emailQueue: Queue;
  private smsQueue: Queue;
  private whatsappQueue: Queue;
  private campaignQueue: Queue;
  private delayQueue: Queue;

  // Workers
  private emailWorker: Worker;
  private smsWorker: Worker;
  private whatsappWorker: Worker;
  private campaignWorker: Worker;
  private delayWorker: Worker;

  // Queue Events
  private queueEvents: Map<string, QueueEvents> = new Map();

  constructor() {
    this.logger = new Logger();
    this.dbManager = new DatabaseManager();
    
    // Initialize Redis connection
    this.redis = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
      retryDelayOnFailedAttempt: (attemptNumber) => Math.min(attemptNumber * 50, 500),
      maxRetriesPerRequest: 3,
    });
  }

  async initialize(): Promise<void> {
    try {
      // Test Redis connection
      await this.redis.ping();
      this.logger.info('Redis connection established');

      // Initialize queues
      await this.initializeQueues();
      
      // Initialize workers
      await this.initializeWorkers();
      
      // Initialize queue events
      this.initializeQueueEvents();

      this.logger.info('Queue manager initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize queue manager:', error);
      throw error;
    }
  }

  private async initializeQueues(): Promise<void> {
    const redisConfig = { connection: this.redis };

    // Create queues with different configurations
    this.emailQueue = new Queue('email-sending', {
      ...redisConfig,
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
        removeOnComplete: 100,
        removeOnFail: 50,
      }
    });

    this.smsQueue = new Queue('sms-sending', {
      ...redisConfig,
      defaultJobOptions: {
        attempts: 5,
        backoff: {
          type: 'exponential',
          delay: 1000,
        },
        removeOnComplete: 100,
        removeOnFail: 50,
      }
    });

    this.whatsappQueue = new Queue('whatsapp-sending', {
      ...redisConfig,
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 3000,
        },
        removeOnComplete: 100,
        removeOnFail: 50,
      }
    });

    this.campaignQueue = new Queue('campaign-processing', {
      ...redisConfig,
      defaultJobOptions: {
        attempts: 2,
        backoff: {
          type: 'fixed',
          delay: 5000,
        },
        removeOnComplete: 50,
        removeOnFail: 25,
      }
    });

    this.delayQueue = new Queue('delay-processing', {
      ...redisConfig,
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: 1000,
        removeOnFail: 100,
      }
    });

    this.logger.info('Queues initialized');
  }

  private async initializeWorkers(): Promise<void> {
    const redisConfig = { connection: this.redis };

    // Initialize processors
    const emailProcessor = new EmailProcessor(this.dbManager, this.logger);
    const smsProcessor = new SMSProcessor(this.dbManager, this.logger);
    const whatsappProcessor = new WhatsAppProcessor(this.dbManager, this.logger);
    const campaignProcessor = new CampaignProcessor(this.dbManager, this.logger, this);

    // Create workers
    this.emailWorker = new Worker('email-sending', async (job) => {
      return emailProcessor.process(job);
    }, {
      ...redisConfig,
      concurrency: parseInt(process.env.EMAIL_WORKER_CONCURRENCY || '5'),
      limiter: {
        max: 100,
        duration: 60000, // 100 jobs per minute
      }
    });

    this.smsWorker = new Worker('sms-sending', async (job) => {
      return smsProcessor.process(job);
    }, {
      ...redisConfig,
      concurrency: parseInt(process.env.SMS_WORKER_CONCURRENCY || '3'),
      limiter: {
        max: 50,
        duration: 60000, // 50 jobs per minute
      }
    });

    this.whatsappWorker = new Worker('whatsapp-sending', async (job) => {
      return whatsappProcessor.process(job);
    }, {
      ...redisConfig,
      concurrency: parseInt(process.env.WHATSAPP_WORKER_CONCURRENCY || '2'),
      limiter: {
        max: 30,
        duration: 60000, // 30 jobs per minute
      }
    });

    this.campaignWorker = new Worker('campaign-processing', async (job) => {
      return campaignProcessor.process(job);
    }, {
      ...redisConfig,
      concurrency: 1, // Only one campaign processing at a time
    });

    this.delayWorker = new Worker('delay-processing', async (job) => {
      return this.processDelayedJob(job);
    }, {
      ...redisConfig,
      concurrency: parseInt(process.env.DELAY_WORKER_CONCURRENCY || '10'),
    });

    // Add error handlers
    this.setupWorkerErrorHandlers();

    this.logger.info('Workers initialized');
  }

  private initializeQueueEvents(): void {
    const queueNames = ['email-sending', 'sms-sending', 'whatsapp-sending', 'campaign-processing', 'delay-processing'];

    queueNames.forEach(queueName => {
      const queueEvents = new QueueEvents(queueName, { connection: this.redis });
      
      queueEvents.on('completed', (jobId, returnvalue) => {
        this.logger.info(`Job completed in queue ${queueName}:`, { jobId, returnvalue });
      });

      queueEvents.on('failed', (jobId, err) => {
        this.logger.error(`Job failed in queue ${queueName}:`, { jobId, error: err });
      });

      queueEvents.on('progress', (jobId, progress) => {
        this.logger.debug(`Job progress in queue ${queueName}:`, { jobId, progress });
      });

      this.queueEvents.set(queueName, queueEvents);
    });

    this.logger.info('Queue events initialized');
  }

  private setupWorkerErrorHandlers(): void {
    const workers = [
      { worker: this.emailWorker, name: 'email' },
      { worker: this.smsWorker, name: 'sms' },
      { worker: this.whatsappWorker, name: 'whatsapp' },
      { worker: this.campaignWorker, name: 'campaign' },
      { worker: this.delayWorker, name: 'delay' }
    ];

    workers.forEach(({ worker, name }) => {
      worker.on('completed', (job, result) => {
        this.logger.info(`${name} worker job completed:`, { jobId: job.id, result });
      });

      worker.on('failed', (job, err) => {
        this.logger.error(`${name} worker job failed:`, { jobId: job?.id, error: err });
      });

      worker.on('error', (err) => {
        this.logger.error(`${name} worker error:`, err);
      });
    });
  }

  // Job scheduling methods
  async scheduleEmailJob(messageData: any, delayMs: number = 0): Promise<Job> {
    return this.emailQueue.add('send-email', messageData, {
      delay: delayMs,
      jobId: `email-${messageData.messageId}-${Date.now()}`
    });
  }

  async scheduleSMSJob(messageData: any, delayMs: number = 0): Promise<Job> {
    return this.smsQueue.add('send-sms', messageData, {
      delay: delayMs,
      jobId: `sms-${messageData.messageId}-${Date.now()}`
    });
  }

  async scheduleWhatsAppJob(messageData: any, delayMs: number = 0): Promise<Job> {
    return this.whatsappQueue.add('send-whatsapp', messageData, {
      delay: delayMs,
      jobId: `whatsapp-${messageData.messageId}-${Date.now()}`
    });
  }

  async scheduleCampaignJob(campaignData: any): Promise<Job> {
    return this.campaignQueue.add('process-campaign', campaignData, {
      jobId: `campaign-${campaignData.campaignId}-${Date.now()}`
    });
  }

  async scheduleDelayedJob(jobData: any, delayMs: number): Promise<Job> {
    return this.delayQueue.add('delayed-job', jobData, {
      delay: delayMs,
      jobId: `delayed-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
    });
  }

  // Delayed job processor with random delay calculation
  private async processDelayedJob(job: Job): Promise<any> {
    const { targetQueue, targetJobType, messageData, delayConfig, campaignId, messageId } = job.data;

    // Calculate random delay based on configuration
    const delayMs = this.calculateRandomDelay(delayConfig);
    
    // Log the delay for audit purposes (compliance requirement)
    this.logger.delayLog(messageId, campaignId, delayMs / 1000, delayConfig.distribution);

    // Wait for the calculated delay
    await new Promise(resolve => setTimeout(resolve, delayMs));

    // Schedule the actual job
    switch (targetQueue) {
      case 'email':
        return this.scheduleEmailJob(messageData, 0);
      case 'sms':
        return this.scheduleSMSJob(messageData, 0);
      case 'whatsapp':
        return this.scheduleWhatsAppJob(messageData, 0);
      default:
        throw new Error(`Unknown target queue: ${targetQueue}`);
    }
  }

  // Random delay calculation method (compliance focused)
  private calculateRandomDelay(delayConfig: any): number {
    const { minDelaySeconds, maxDelaySeconds, distribution } = delayConfig;
    
    if (!delayConfig.enabled) {
      return 0;
    }

    const minMs = minDelaySeconds * 1000;
    const maxMs = maxDelaySeconds * 1000;

    switch (distribution) {
      case 'uniform':
        return Math.random() * (maxMs - minMs) + minMs;
        
      case 'normal':
        const { normalMean, normalStd } = delayConfig;
        const u1 = Math.random();
        const u2 = Math.random();
        const z0 = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
        const normalValue = (normalMean || (minDelaySeconds + maxDelaySeconds) / 2) + z0 * (normalStd || (maxDelaySeconds - minDelaySeconds) / 6);
        return Math.max(minMs, Math.min(maxMs, normalValue * 1000));
        
      case 'exponential':
        const { exponentialLambda } = delayConfig;
        const lambda = exponentialLambda || 1 / ((minDelaySeconds + maxDelaySeconds) / 2);
        const exponentialValue = -Math.log(1 - Math.random()) / lambda;
        return Math.max(minMs, Math.min(maxMs, exponentialValue * 1000));
        
      default:
        return Math.random() * (maxMs - minMs) + minMs;
    }
  }

  // Queue monitoring methods
  async getQueueStats(): Promise<any> {
    const queues = [
      { name: 'email-sending', queue: this.emailQueue },
      { name: 'sms-sending', queue: this.smsQueue },
      { name: 'whatsapp-sending', queue: this.whatsappQueue },
      { name: 'campaign-processing', queue: this.campaignQueue },
      { name: 'delay-processing', queue: this.delayQueue }
    ];

    const stats: any = {};

    for (const { name, queue } of queues) {
      const [waiting, active, completed, failed, delayed] = await Promise.all([
        queue.getWaiting(),
        queue.getActive(),
        queue.getCompleted(),
        queue.getFailed(),
        queue.getDelayed()
      ]);

      stats[name] = {
        waiting: waiting.length,
        active: active.length,
        completed: completed.length,
        failed: failed.length,
        delayed: delayed.length
      };
    }

    return stats;
  }

  async pauseQueue(queueName: string): Promise<void> {
    const queue = this.getQueueByName(queueName);
    await queue.pause();
    this.logger.info(`Queue paused: ${queueName}`);
  }

  async resumeQueue(queueName: string): Promise<void> {
    const queue = this.getQueueByName(queueName);
    await queue.resume();
    this.logger.info(`Queue resumed: ${queueName}`);
  }

  private getQueueByName(queueName: string): Queue {
    switch (queueName) {
      case 'email-sending':
        return this.emailQueue;
      case 'sms-sending':
        return this.smsQueue;
      case 'whatsapp-sending':
        return this.whatsappQueue;
      case 'campaign-processing':
        return this.campaignQueue;
      case 'delay-processing':
        return this.delayQueue;
      default:
        throw new Error(`Unknown queue: ${queueName}`);
    }
  }

  async shutdown(): Promise<void> {
    try {
      // Close workers
      await Promise.all([
        this.emailWorker?.close(),
        this.smsWorker?.close(),
        this.whatsappWorker?.close(),
        this.campaignWorker?.close(),
        this.delayWorker?.close()
      ]);

      // Close queue events
      for (const queueEvents of this.queueEvents.values()) {
        await queueEvents.close();
      }

      // Close queues
      await Promise.all([
        this.emailQueue?.close(),
        this.smsQueue?.close(),
        this.whatsappQueue?.close(),
        this.campaignQueue?.close(),
        this.delayQueue?.close()
      ]);

      // Close Redis connection
      await this.redis.disconnect();

      this.logger.info('Queue manager shutdown complete');
    } catch (error) {
      this.logger.error('Error during queue manager shutdown:', error);
      throw error;
    }
  }
}