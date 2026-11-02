package writer

import (
	"context"
	"db-etl/config"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// defaultAzureBlockSizeMB / defaultAzureConcurrency 是分块上传的默认参数。
// 8MiB × 4 并发是经验值：块太小会增加请求数（HTTP 开销在 DMZ/跨网段场景被放大），
// 块太大则单块失败重传的代价更高；并发数按「DMZ 带宽通常是瓶颈」保守取值，
// 可按 azure_blob[].block_size_mb / concurrency 针对具体网络条件调优。
const (
	defaultAzureBlockSizeMB = 8
	defaultAzureConcurrency = 4
)

// azureBlobStore 通过 Azure SDK 将对象流式上传到 Azure Blob Storage（block blob）。
// 与 s3Store 实现同一个 objectStore 接口，供 parquetWriterDialect 无差别调用；
// 二者的差异（S3 协议 vs Azure REST API、认证方式）完全封装在各自的 put/describe 内。
type azureBlobStore struct {
	client      *azblob.Client
	container   string
	prefix      string
	blockSizeMB int
	concurrency int
}

// newAzureBlobStore 依据 azure_blob 配置构建 Azure Blob 客户端。
// 认证优先级：connection_string > account_name+sas_token > account_name+account_key
// （与 config.AzureBlobConfig.Validate 的必填校验顺序一致）。
func newAzureBlobStore(cfg config.AzureBlobConfig) (*azureBlobStore, error) {
	client, err := newAzureClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("init azure blob client for storage %q failed: %w", cfg.Name, err)
	}

	blockSizeMB := cfg.BlockSizeMB
	if blockSizeMB <= 0 {
		blockSizeMB = defaultAzureBlockSizeMB
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = defaultAzureConcurrency
	}

	return &azureBlobStore{
		client:      client,
		container:   cfg.Container,
		prefix:      strings.Trim(strings.TrimSpace(cfg.Prefix), "/"),
		blockSizeMB: blockSizeMB,
		concurrency: concurrency,
	}, nil
}

func newAzureClient(cfg config.AzureBlobConfig) (*azblob.Client, error) {
	switch {
	case strings.TrimSpace(cfg.ConnectionString) != "":
		return azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)

	case strings.TrimSpace(cfg.SASToken) != "":
		serviceURL := azureServiceURL(cfg)
		sas := strings.TrimPrefix(strings.TrimSpace(cfg.SASToken), "?")
		// SAS 是「凭证即 URL 查询串」的只读/受限权限令牌：不走共享密钥签名，
		// 直接拼进服务 URL 即可认证，适合 DMZ 场景下只下发最小权限、有有效期的令牌。
		return azblob.NewClientWithNoCredential(serviceURL+"?"+sas, nil)

	case strings.TrimSpace(cfg.AccountKey) != "":
		cred, err := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
		if err != nil {
			return nil, err
		}
		return azblob.NewClientWithSharedKeyCredential(azureServiceURL(cfg), cred, nil)

	default:
		// 不会到达：config.AzureBlobConfig.Validate 已确保三者至少其一非空。
		return nil, fmt.Errorf("azure_blob storage requires connection_string, account_name+sas_token, or account_name+account_key")
	}
}

// azureServiceURL 返回 blob 服务端点：显式配置优先，否则按公有云默认域名推导。
// 国内版（.chinacloudapi.cn）、政务云或私有化部署需显式设置 service_url。
func azureServiceURL(cfg config.AzureBlobConfig) string {
	if u := strings.TrimSpace(cfg.ServiceURL); u != "" {
		return strings.TrimRight(u, "/")
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net", cfg.AccountName)
}

// objectName 将相对 key 拼上配置的前缀，得到最终 blob 名。
func (a *azureBlobStore) objectName(key string) string {
	if a.prefix == "" {
		return key
	}
	return a.prefix + "/" + key
}

// put 从 r 流式读取数据并上传为 key 标识的 block blob（key 相同即覆盖）。
// 走分块并发上传（UploadStream），避免整段数据在内存中缓冲，与 s3Store.put 的流式语义保持一致。
func (a *azureBlobStore) put(ctx context.Context, key string, r io.Reader) error {
	_, err := a.client.UploadStream(ctx, a.container, a.objectName(key), r, &azblob.UploadStreamOptions{
		BlockSize:   int64(a.blockSizeMB) * 1024 * 1024,
		Concurrency: a.concurrency,
	})
	if err != nil {
		return fmt.Errorf("upload object %s failed: %w", a.describe(key), err)
	}
	return nil
}

// describe 返回 key 对应的人类可读目标位置，用于日志。
func (a *azureBlobStore) describe(key string) string {
	return fmt.Sprintf("azblob://%s/%s", a.container, a.objectName(key))
}

var _ objectStore = (*azureBlobStore)(nil)
