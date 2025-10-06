import { RedisConfig } from './config';
import { createClient, createCluster } from 'redis';
import { delay } from './util';
import { Static, Type } from '@sinclair/typebox';
import { TypeCompiler } from '@sinclair/typebox/compiler';
import { PackageListener } from './packageListener';
import logger from './logger';

export const QueueMessage = Type.Object({
  jobId: Type.String(),
  url: Type.String()
});

export type QueueMessage = Static<typeof QueueMessage>;

const QueueMessageChecker = TypeCompiler.Compile(QueueMessage);

export function validateQueueMessage(message: unknown) {
  if (!QueueMessageChecker.Check(message)) {
    const errors = QueueMessageChecker.Errors(message);
    throw new Error(`Invalid message: ${errors}`);
  }
}

export class RedisListener {
  private running = false;
  private noProcessing = 0;

  private client: Awaited<ReturnType<typeof createClient>> | undefined;
  private cluster: Awaited<ReturnType<typeof createCluster>> | undefined;
  private uploadClient: Awaited<ReturnType<typeof createClient>> | undefined;
  private metadataClient: Awaited<ReturnType<typeof createClient>> | undefined; // NEW: Client for retrieving original S3 path

  constructor(
    private redisConfig: RedisConfig,
    private onMessage: (message: QueueMessage) => Promise<string | void>,
    private concurrency: number = 1,
    private packageListener?: PackageListener
  ) {}

  async start() {
    this.running = true;
    while (this.running) {
      try {
        await this.connect();
        if (this.redisConfig.clusterMode) {
          logger.info('Connected to Redis Cluster');
        }

        if (this.noProcessing < this.concurrency) {
          let message;
          try {
            logger.info('Waiting for message...');
            message = this.redisConfig.clusterMode
              ? await this.cluster?.bzPopMin(this.redisConfig.queueName, 2000)
              : await this.client?.bzPopMin(this.redisConfig.queueName, 2000);
          } catch (err) {
            logger.error(err);
          }
          if (message) {
            this.handleMessage(message.value);
          } else {
            logger.info('No message received, waiting...');
          }
        } else {
          await delay(1000);
        }
      } catch (err) {
        logger.error(`Error when processing queue: ${(err as Error)?.message}`);
        await delay(3000);
      }
    }
  }

  async stop() {
    this.running = false;
    await this.disconnect();
    await this.uploadClient?.quit();
    await this.metadataClient?.quit(); // NEW: Clean up metadata client
  }

  async handleMessage(message: string) {
    try {
      logger.info(`Received message: ${message}`);
      const parsedMessage = JSON.parse(message);
      validateQueueMessage(parsedMessage);
      this.noProcessing++;
      try {
        logger.info(
          `Sending message for processing, currently processing ${this.noProcessing} messages`
        );
        this.onPackageStart(parsedMessage.url, parsedMessage.jobId);
        const onMessageResult = await this.onMessage(parsedMessage);
        this.onPackageDone(
          parsedMessage.url,
          parsedMessage.jobId,
          onMessageResult ? onMessageResult : undefined
        );
      } finally {
        this.noProcessing--;
      }
    } catch (e) {
      logger.error(
        `Error when handling message ${message}: ${(e as Error)?.message}`
      );
      this.onPackageFail(message, e);
    }
  }

  async connect() {
    if (this.redisConfig.clusterMode) {
      if (this.cluster) {
        return;
      }
      this.cluster = await createCluster({
        rootNodes: [{ url: this.redisConfig.url }]
      }).on('error', (err) => {
        logger.warn(`Redis Cluster Error: ${(err as Error).message}`);
      });
      await this.cluster.connect();
    } else {
      if (this.client) {
        return;
      }
      this.client = await createClient({ url: this.redisConfig.url })
        .on('error', (err) => {
          logger.warn(`Redis Client Error: ${(err as Error).message}`);
        })
        .connect();
    }

    // Initialize both upload and metadata clients
    await this.initUploadClient();
    await this.initMetadataClient(); // NEW: Initialize metadata client
  }

  async disconnect() {
    if (this.redisConfig.clusterMode) {
      await this.cluster?.quit();
      this.cluster = undefined;
    }
    await this.client?.quit();
    this.client = undefined;
    await this.uploadClient?.quit();
    this.uploadClient = undefined;
    await this.metadataClient?.quit(); // NEW: Clean up metadata client
    this.metadataClient = undefined;
  }

  // Initialize upload notification client
  private async initUploadClient() {
    const uploadEnabled = process.env.UPLOAD_ENABLED === 'true';
    if (uploadEnabled && !this.uploadClient) {
      try {
        this.uploadClient = await createClient({ 
          url: this.redisConfig.url 
        })
        .on('error', (err) => {
          logger.warn(`Upload Redis Client Error: ${(err as Error).message}`);
        })
        .connect();
        logger.info('✅ Upload notification client connected');
      } catch (error) {
        logger.warn(`Failed to connect upload client: ${error}`);
      }
    }
  }

  // NEW: Initialize metadata client for retrieving original S3 path
  private async initMetadataClient() {
    const uploadEnabled = process.env.UPLOAD_ENABLED === 'true';
    if (uploadEnabled && !this.metadataClient) {
      try {
        this.metadataClient = await createClient({ 
          url: this.redisConfig.url 
        })
        .on('error', (err) => {
          logger.warn(`Metadata Redis Client Error: ${(err as Error).message}`);
        })
        .connect();
        logger.info('✅ Metadata client connected');
      } catch (error) {
        logger.warn(`Failed to connect metadata client: ${error}`);
      }
    }
  }

  // NEW: Retrieve original S3 path from Redis
  // private async getOriginalS3Path(jobId: string): Promise<string | null> {
  //   try {
  //     if (!this.metadataClient) {
  //       logger.warn('Metadata client not available');
  //       return null;
  //     }

  //     const redisKey = `original-s3-path:${jobId}`;
  //     const originalPath = await this.metadataClient.get(redisKey);
      
  //     if (originalPath) {
  //       logger.info(`📁 Retrieved original S3 path for job ${jobId}: ${originalPath}`);
  //       return originalPath;
  //     } else {
  //       logger.warn(`❌ No original S3 path found for job ${jobId}`);
  //       return null;
  //     }
  //   } catch (error) {
  //     logger.warn(`Failed to retrieve original S3 path for job ${jobId}: ${error}`);
  //     return null;
  //   }
  // }

  // NEW: Retrieve original S3 path from Redis using external ID
private async getOriginalS3Path(jobId: string): Promise<string | null> {
  try {
    if (!this.metadataClient) {
      logger.warn('Metadata client not available');
      return null;
    }

    // First, get the job details from Encore to extract the externalId
    const jobUrl = `http://encore:8080/encoreJobs/${jobId}`;
    logger.info(`🔍 Fetching job details from: ${jobUrl}`);
    
    const response = await fetch(jobUrl);
    if (!response.ok) {
      logger.warn(`Failed to fetch job details for ${jobId}: ${response.statusText}`);
      return null;
    }

    const jobDetails = await response.json();
    const externalId = jobDetails.externalId;
    
    if (!externalId) {
      logger.warn(`❌ No externalId found in job details for job ${jobId}`);
      return null;
    }

    logger.info(`📁 Looking up S3 path for externalId: ${externalId}`);
    
    // Use the externalId to look up the original S3 path
    const redisKey = `originalS3Path:${externalId}`;
    const originalPath = await this.metadataClient.get(redisKey);
    
    if (originalPath) {
      logger.info(`✅ Retrieved original S3 path for job ${jobId}: ${originalPath}`);
      return originalPath;
    } else {
      logger.warn(`❌ No original S3 path found for externalId ${externalId} (job ${jobId})`);
      return null;
    }
  } catch (error) {
    logger.warn(`Failed to retrieve original S3 path for job ${jobId}: ${error}`);
    return null;
  }
}

  // Publish upload notification with original S3 path
  private async publishUploadNotification(jobId: string, outputPath?: string) {
    try {
      const uploadEnabled = process.env.UPLOAD_ENABLED === 'true';
      
      if (!uploadEnabled || !outputPath || !this.uploadClient) {
        return;
      }

      // NEW: Retrieve original S3 path
      const originalS3Path = await this.getOriginalS3Path(jobId);
      
      const packagesBaseDir = process.env.PACKAGES_BASE_DIR || '/data/packages';
      const relativePath = outputPath.replace(packagesBaseDir, '').replace(/^\//, '');
      const uploadChannel = process.env.UPLOAD_REDIS_CHANNEL || 'packaging-complete';
      
      if (relativePath) {
        const uploadMessage = {
          jobId: jobId,
          packagePath: relativePath,
          timestamp: new Date().toISOString(),
          originalS3Path: originalS3Path // NEW: Include original S3 path
        };

        await this.uploadClient.publish(uploadChannel, JSON.stringify(uploadMessage));
        
        if (originalS3Path) {
          logger.info(`📤 Published upload notification for: ${relativePath} (original: ${originalS3Path})`);
        } else {
          logger.info(`📤 Published upload notification for: ${relativePath} (no original path found)`);
        }
      }
    } catch (error) {
      logger.warn(`Failed to publish upload notification: ${error}`);
    }
  }

  redisStatus(): 'UP' | 'DOWN' {
    if (this.redisConfig.clusterMode) {
      return 'UP';
    }
    if (!this.client) {
      return 'UP';
    }
    return this.client.isReady ? 'UP' : 'DOWN';
  }

  onPackageStart(jobUrl: string, jobId: string) {
    try {
      this.packageListener?.onPackageStart?.(jobUrl, jobId);
    } catch (err) {
      logger.warn(
        `Error when calling beforePackage: ${(err as Error).message}`
      );
    }
  }

  onPackageDone(jobUrl: string, jobId: string, outputPath?: string) {
    try {
      this.packageListener?.onPackageDone?.(jobUrl, jobId, outputPath);
      
      // Trigger upload notification with original S3 path
      this.publishUploadNotification(jobId, outputPath);
    } catch (err) {
      logger.warn(
        `Error when calling onPackageDone: ${(err as Error).message}`
      );
    }
  }

  //eslint-disable-next-line @typescript-eslint/no-explicit-any
  onPackageFail(message: string, err: any, jobId?: string) {
    try {
      this.packageListener?.onPackageFail?.(message, err);
    } catch (e) {
      logger.warn(`Error when calling onPackageFail: ${(e as Error).message}`);
    }
  }
}
