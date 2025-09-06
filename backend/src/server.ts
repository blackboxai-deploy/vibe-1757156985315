import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import dotenv from 'dotenv';
import { createServer } from 'http';
import { Server as SocketIOServer } from 'socket.io';
import path from 'path';

// Import routes
import authRoutes from './routes/auth';
import contactRoutes from './routes/contacts';
import campaignRoutes from './routes/campaigns';
import accountRoutes from './routes/accounts';
import deviceRoutes from './routes/devices';
import templateRoutes from './routes/templates';
import reportRoutes from './routes/reports';
import googleRoutes from './routes/google';
import webhookRoutes from './routes/webhooks';
import systemRoutes from './routes/system';

// Import middleware
import { errorHandler } from './middleware/errorHandler';
import { rateLimiter } from './middleware/rateLimiter';
import { authenticateToken } from './middleware/auth';
import { requestLogger } from './middleware/requestLogger';

// Import services
import { DatabaseManager } from './services/DatabaseManager';
import { QueueManager } from './services/QueueManager';
import { SocketManager } from './services/SocketManager';
import { DeviceManager } from './services/DeviceManager';
import { Logger } from './services/Logger';

// Load environment variables
dotenv.config();

const app = express();
const server = createServer(app);
const io = new SocketIOServer(server, {
  cors: {
    origin: process.env.FRONTEND_URL || "http://localhost:3000",
    methods: ["GET", "POST"]
  }
});

const PORT = process.env.BACKEND_PORT || 5000;
const logger = new Logger();

// Initialize managers
const databaseManager = new DatabaseManager();
const queueManager = new QueueManager();
const socketManager = new SocketManager(io);
const deviceManager = new DeviceManager();

// Security middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'", "ws:", "wss:"],
      fontSrc: ["'self'"],
      objectSrc: ["'none'"],
      mediaSrc: ["'self'"],
      frameSrc: ["'none'"],
    },
  },
}));

app.use(cors({
  origin: process.env.FRONTEND_URL || "http://localhost:3000",
  credentials: true
}));

// Request parsing middleware
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Logging middleware
app.use(requestLogger);

// Rate limiting
app.use('/api/', rateLimiter);

// Health check endpoint (public)
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    version: process.env.npm_package_version || '1.0.0'
  });
});

// Public routes (no auth required)
app.use('/api/auth', authRoutes);
app.use('/api/webhooks', webhookRoutes);
app.use('/api/google/oauth', googleRoutes);

// Protected routes (auth required)
app.use('/api/contacts', authenticateToken, contactRoutes);
app.use('/api/campaigns', authenticateToken, campaignRoutes);
app.use('/api/accounts', authenticateToken, accountRoutes);
app.use('/api/devices', authenticateToken, deviceRoutes);
app.use('/api/templates', authenticateToken, templateRoutes);
app.use('/api/reports', authenticateToken, reportRoutes);
app.use('/api/system', authenticateToken, systemRoutes);

// Static file serving for uploads
app.use('/uploads', express.static(path.join(__dirname, '../uploads')));

// Error handling middleware
app.use(errorHandler);

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({ 
    success: false, 
    error: 'Route not found',
    path: req.originalUrl,
    method: req.method
  });
});

// Socket.IO connection handling
io.on('connection', (socket) => {
  logger.info(`Client connected: ${socket.id}`);
  
  socket.on('join_room', (room) => {
    socket.join(room);
    logger.info(`Client ${socket.id} joined room: ${room}`);
  });
  
  socket.on('leave_room', (room) => {
    socket.leave(room);
    logger.info(`Client ${socket.id} left room: ${room}`);
  });
  
  socket.on('disconnect', () => {
    logger.info(`Client disconnected: ${socket.id}`);
  });
});

// Graceful shutdown handling
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down gracefully...');
  
  // Stop accepting new connections
  server.close(async () => {
    try {
      // Stop queue processing
      await queueManager.shutdown();
      
      // Close database connections
      await databaseManager.close();
      
      // Close device connections
      await deviceManager.shutdown();
      
      logger.info('Server shutdown complete');
      process.exit(0);
    } catch (error) {
      logger.error('Error during shutdown:', error);
      process.exit(1);
    }
  });
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down gracefully...');
  
  server.close(async () => {
    try {
      await queueManager.shutdown();
      await databaseManager.close();
      await deviceManager.shutdown();
      
      logger.info('Server shutdown complete');
      process.exit(0);
    } catch (error) {
      logger.error('Error during shutdown:', error);
      process.exit(1);
    }
  });
});

// Initialize services and start server
async function startServer() {
  try {
    // Initialize database
    logger.info('Initializing database...');
    await databaseManager.initialize();
    
    // Initialize queue manager
    logger.info('Initializing queue manager...');
    await queueManager.initialize();
    
    // Initialize device manager
    logger.info('Initializing device manager...');
    await deviceManager.initialize();
    
    // Start server
    server.listen(PORT, () => {
      logger.info(`Server running on port ${PORT}`);
      logger.info(`Environment: ${process.env.NODE_ENV || 'development'}`);
      logger.info(`Frontend URL: ${process.env.FRONTEND_URL || 'http://localhost:3000'}`);
      logger.info(`Database: ${process.env.DATABASE_TYPE || 'sqlite'}`);
      logger.info(`Redis URL: ${process.env.REDIS_URL || 'redis://localhost:6379'}`);
    });
    
  } catch (error) {
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Export for testing
export { app, server, io };

// Start the server if this file is run directly
if (require.main === module) {
  startServer();
}