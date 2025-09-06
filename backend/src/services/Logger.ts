import winston from 'winston';
import path from 'path';

export class Logger {
  private logger: winston.Logger;

  constructor() {
    // Ensure logs directory exists
    const logDir = path.join(__dirname, '../../logs');
    
    // Create winston logger with multiple transports
    this.logger = winston.createLogger({
      level: process.env.LOG_LEVEL || 'info',
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json(),
        winston.format.prettyPrint()
      ),
      defaultMeta: { service: 'messaging-suite-backend' },
      transports: [
        // Write all logs with level 'error' and below to error.log
        new winston.transports.File({ 
          filename: path.join(logDir, 'error.log'), 
          level: 'error',
          maxsize: 10485760, // 10MB
          maxFiles: 5
        }),
        
        // Write all logs to combined.log
        new winston.transports.File({ 
          filename: path.join(logDir, 'combined.log'),
          maxsize: 10485760, // 10MB
          maxFiles: 10
        }),
        
        // Write audit logs separately
        new winston.transports.File({ 
          filename: path.join(logDir, 'audit.log'),
          level: 'info',
          maxsize: 10485760, // 10MB
          maxFiles: 20
        })
      ],
    });

    // Add console transport for development
    if (process.env.NODE_ENV !== 'production') {
      this.logger.add(new winston.transports.Console({
        format: winston.format.combine(
          winston.format.colorize(),
          winston.format.simple(),
          winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
          winston.format.printf(({ timestamp, level, message, ...meta }) => {
            return `${timestamp} [${level}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}`;
          })
        )
      }));
    }
  }

  info(message: string, meta?: any) {
    this.logger.info(message, meta);
  }

  error(message: string, error?: any) {
    this.logger.error(message, { error: error?.stack || error });
  }

  warn(message: string, meta?: any) {
    this.logger.warn(message, meta);
  }

  debug(message: string, meta?: any) {
    this.logger.debug(message, meta);
  }

  audit(action: string, userId?: string, resourceType?: string, resourceId?: string, details?: any) {
    this.logger.info('AUDIT', {
      action,
      userId,
      resourceType,
      resourceId,
      details,
      timestamp: new Date().toISOString()
    });
  }

  // Method for logging message processing
  messageLog(messageId: string, campaignId: string, action: string, details?: any) {
    this.logger.info('MESSAGE_PROCESSING', {
      messageId,
      campaignId,
      action,
      details,
      timestamp: new Date().toISOString()
    });
  }

  // Method for logging delay usage (compliance requirement)
  delayLog(messageId: string, campaignId: string, delayUsed: number, distribution: string) {
    this.logger.info('DELAY_AUDIT', {
      messageId,
      campaignId,
      delayUsed,
      distribution,
      timestamp: new Date().toISOString(),
      purpose: 'load_smoothing_and_human_timing'
    });
  }

  // Method for logging device operations
  deviceLog(deviceId: string, action: string, details?: any) {
    this.logger.info('DEVICE_OPERATION', {
      deviceId,
      action,
      details,
      timestamp: new Date().toISOString()
    });
  }

  // Method for logging OAuth operations
  oauthLog(provider: string, action: string, userId?: string, details?: any) {
    this.logger.info('OAUTH_OPERATION', {
      provider,
      action,
      userId,
      details: { ...details, tokens: '[REDACTED]' }, // Never log actual tokens
      timestamp: new Date().toISOString()
    });
  }

  // Method for logging rate limiting events
  rateLimitLog(identifier: string, action: string, limit: number, current: number) {
    this.logger.warn('RATE_LIMIT_EVENT', {
      identifier,
      action,
      limit,
      current,
      timestamp: new Date().toISOString()
    });
  }

  // Method for logging security events
  securityLog(event: string, ipAddress?: string, userAgent?: string, details?: any) {
    this.logger.warn('SECURITY_EVENT', {
      event,
      ipAddress,
      userAgent,
      details,
      timestamp: new Date().toISOString()
    });
  }
}