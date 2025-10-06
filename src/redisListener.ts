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
  private metadataClient: Awaited<ReturnType<typeof createClient>> | undefined;

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
    await this.metadataClient?.quit();
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
        
        // Enhanced debugging for onMessage result
        console.log(`=== 🎯 CALLING ONMESSAGE FUNCTION ===`);
        const onMessageResult = await this.onMessage(parsedMessage);
        console.log(`✅ onMessage completed`);
        console.log(`onMessage result: ${onMessageResult || 'UNDEFINED'}`);
        console.log(`onMessage result type: ${typeof onMessageResult}`);
        console.log(`=== 🎯 ONMESSAGE COMPLETED ===\n`);
        
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

    await this.initUploadClient();
    await this.initMetadataClient();
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
    await this.metadataClient?.quit();
    this.metadataClient = undefined;
  }

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

  private async getOriginalS3Path(jobId: string): Promise<string | null> {
  try {
    console.log(`\n=== 🔍 GET ORIGINAL S3 PATH START ===`);
    console.log(`Job ID: ${jobId}`);
    
    if (!this.metadataClient) {
      console.log(`❌ Metadata client not available`);
      return null;
    }

    // Test Redis connectivity
    try {
      await this.metadataClient.ping();
      console.log(`✅ Redis ping successful`);
    } catch (redisError) {
      console.log(`❌ Redis ping failed: ${redisError}`);
      return null;
    }

    const jobUrl = `http://encore:8080/encoreJobs/${jobId}`;
    console.log(`🔍 Fetching job details from: ${jobUrl}`);
    
    const response = await fetch(jobUrl);
    console.log(`📡 Encore API response status: ${response.status} ${response.statusText}`);
    
    if (!response.ok) {
      console.log(`❌ Failed to fetch job details for ${jobId}: ${response.statusText}`);
      return null;
    }

    const jobDetails = await response.json();
    const externalId = jobDetails.externalId;
    
    console.log(`📄 Job details - ID: ${jobId}, ExternalID: ${externalId}`);
    
    if (!externalId) {
      console.log(`❌ No externalId found in job details for job ${jobId}`);
      return null;
    }

    console.log(`📁 Looking up S3 path for externalId: ${externalId}`);
    
    const keyFormats = [
      `originalS3Path:${externalId}`,
      `original-s3-path:${externalId}`
    ];
    
    console.log(`🔑 Trying key formats: ${JSON.stringify(keyFormats)}`);
    
    let originalPath: string | null = null;
    let foundKey: string | null = null;
    
    for (const key of keyFormats) {
      console.log(`   Checking key: ${key}`);
      const value = await this.metadataClient.get(key);
      if (value) {
        originalPath = value;
        foundKey = key;
        console.log(`   ✅ FOUND using key: ${key}`);
        console.log(`   📁 S3 Path: ${value}`);
        break;
      } else {
        console.log(`   ❌ NOT FOUND using key: ${key}`);
      }
    }
    
    if (originalPath && foundKey) {
      console.log(`🎉 SUCCESS: Retrieved original S3 path for job ${jobId}`);
      
      if (foundKey.includes('original-s3-path')) {
        const newKey = `originalS3Path:${externalId}`;
        await this.metadataClient.set(newKey, originalPath, { EX: 86400 });
        await this.metadataClient.del(foundKey);
        console.log(`🔄 Migrated from ${foundKey} to ${newKey}`);
      }
      
      console.log(`=== 🔍 GET ORIGINAL S3 PATH END - FOUND ===\n`);
      return originalPath;
    } else {
      // FIXED: Use console.log instead of logger.warn
      console.log(`❌❌❌ No original S3 path found for externalId ${externalId}`);
      console.log(`=== 🔴 MAIN PACKAGING WARNING: No original S3 path found for job ${jobId} ===`);
      console.log(`=== 🔴 This is the actual warning that appears in logs ===`);
      
      try {
        const allKeys = await this.metadataClient.keys('*');
        const s3Keys = allKeys.filter(key => 
          key.includes('originalS3Path') || key.includes('original-s3-path')
        );
        console.log(`🔍 Available S3 path keys in Redis: ${JSON.stringify(s3Keys)}`);
        
        // Show values of available keys
        for (const key of s3Keys) {
          const value = await this.metadataClient.get(key);
          console.log(`   ${key} => ${value}`);
        }
      } catch (keysError) {
        console.log(`Failed to list Redis keys: ${keysError}`);
      }
      
      console.log(`=== 🔍 GET ORIGINAL S3 PATH END - NOT FOUND ===\n`);
      return null;
    }
  } catch (error) {
    console.log(`🚨 Failed to retrieve original S3 path for job ${jobId}: ${error}`);
    return null;
  }
}

  private async publishUploadNotification(jobId: string, outputPath?: string) {
  try {
    console.log(`\n\n=== 🚀 PUBLISH UPLOAD NOTIFICATION START ===`);
    console.log(`📋 Job ID: ${jobId}`);
    console.log(`📋 Output Path: ${outputPath || 'UNDEFINED'}`);
    
    // IMMEDIATELY lookup the S3 path to see if it works
    console.log(`🔍 IMMEDIATE S3 PATH LOOKUP FOR JOB: ${jobId}`);
    const immediateS3Path = await this.getOriginalS3Path(jobId);
    console.log(`🔍 IMMEDIATE RESULT: ${immediateS3Path || 'NOT FOUND'}`);
    
    const uploadEnabled = process.env.UPLOAD_ENABLED === 'true';
    console.log(`⚙️ Upload Enabled: ${uploadEnabled}`);
    console.log(`⚙️ Upload Client Available: ${!!this.uploadClient}`);
    
    if (!uploadEnabled) {
      console.log(`❌ Upload not enabled, skipping`);
      return;
    }
    
    if (!outputPath) {
      console.log(`❌ Output path is undefined, cannot proceed`);
      return;
    }
    
    if (!this.uploadClient) {
      console.log(`❌ Upload client not available, skipping`);
      return;
    }

    // Now do the normal lookup
    console.log(`🔍 NORMAL S3 PATH LOOKUP FOR JOB: ${jobId}`);
    const originalS3Path = await this.getOriginalS3Path(jobId);
    console.log(`🔍 NORMAL RESULT: ${originalS3Path || 'NOT FOUND'}`);
    
    const packagesBaseDir = process.env.PACKAGES_BASE_DIR || '/data/packages';
    console.log(`📦 Packages base directory: ${packagesBaseDir}`);
    
    const relativePath = outputPath.replace(packagesBaseDir, '').replace(/^\//, '');
    console.log(`📁 Relative path: "${relativePath}"`);
    
    const uploadChannel = process.env.UPLOAD_REDIS_CHANNEL || 'packaging-complete';
    console.log(`📢 Upload channel: ${uploadChannel}`);
    
    if (relativePath) {
      const uploadMessage = {
        jobId: jobId,
        packagePath: relativePath,
        timestamp: new Date().toISOString(),
        originalS3Path: originalS3Path
      };

      console.log(`📦 FINAL UPLOAD MESSAGE:`, JSON.stringify(uploadMessage, null, 2));
      
      try {
        console.log(`🚀 Publishing to Redis channel: ${uploadChannel}`);
        await this.uploadClient.publish(uploadChannel, JSON.stringify(uploadMessage));
        console.log(`✅ SUCCESS: Published upload notification`);
        
        if (originalS3Path) {
          console.log(`🎯 UPLOADING TO: ${originalS3Path}`);
        } else {
          console.log(`⚠️ UPLOADING TO FALLBACK PATH`);
        }
      } catch (publishError) {
        console.error(`❌ FAILED to publish message: ${publishError}`);
      }
    } else {
      console.warn(`❌ Cannot publish - relativePath is empty`);
    }
    
    console.log(`=== 🚀 PUBLISH UPLOAD NOTIFICATION END ===\n\n`);
  } catch (error) {
    console.error(`🚨 CRITICAL ERROR in publishUploadNotification: ${error}`);
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
      console.log(`=== 🎯 ON PACKAGE DONE CALLED ===`);
      console.log(`Job URL: ${jobUrl}`);
      console.log(`Job ID: ${jobId}`);
      console.log(`Output Path: ${outputPath || 'UNDEFINED'}`);
      console.log(`Output Path Type: ${typeof outputPath}`);
      console.log(`Package Listener Available: ${!!this.packageListener}`);
      
      if (this.packageListener?.onPackageDone) {
        console.log(`📞 Calling packageListener.onPackageDone...`);
        this.packageListener.onPackageDone(jobUrl, jobId, outputPath);
        console.log(`✅ packageListener.onPackageDone completed`);
      } else {
        console.log(`ℹ️ No package listener or onPackageDone method`);
      }
      
      console.log(`🚀 Calling publishUploadNotification...`);
      this.publishUploadNotification(jobId, outputPath);
      console.log(`✅ publishUploadNotification called`);
      console.log(`=== 🎯 ON PACKAGE DONE COMPLETED ===\n`);
    } catch (err) {
      console.error(`❌ Error in onPackageDone: ${(err as Error).message}`);
    }
  }

  onPackageFail(message: string, err: any, jobId?: string) {
    try {
      this.packageListener?.onPackageFail?.(message, err);
    } catch (e) {
      logger.warn(`Error when calling onPackageFail: ${(e as Error).message}`);
    }
  }
}
