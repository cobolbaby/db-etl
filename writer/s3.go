package writer

import (
	"context"
	"db-etl/config"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// s3Store 通过 minio-go 将对象流式上传到 S3 / S3 兼容对象存储。
// 它与 parquet 序列化解耦：dialect 只管往 io.Writer 写，s3Store 从管道另一端流式上传，
// 免去「先落本地临时文件再上传」的二次写入。
type s3Store struct {
	client *minio.Client
	bucket string
	prefix string
}

// newS3Store 依据 s3 配置构建对象存储客户端。
func newS3Store(s3 config.S3Config) (*s3Store, error) {
	client, err := minio.New(s3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3.AccessKey, s3.SecretKey, ""),
		Secure: s3.UseSSL,
		Region: s3.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("init s3 client for storage %q failed: %w", s3.Name, err)
	}

	return &s3Store{
		client: client,
		bucket: s3.Bucket,
		prefix: strings.Trim(strings.TrimSpace(s3.Prefix), "/"),
	}, nil
}

// objectName 将相对 key 拼上配置的前缀，得到最终对象名。
func (s *s3Store) objectName(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// put 从 r 流式读取数据并上传为 key 标识的对象（key 相同即覆盖）。
func (s *s3Store) put(ctx context.Context, key string, r io.Reader) error {
	// objectSize 传 -1：数据大小未知，minio 走分块（multipart）流式上传。
	_, err := s.client.PutObject(ctx, s.bucket, s.objectName(key), r, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("upload object %s failed: %w", s.describe(key), err)
	}
	return nil
}

// describe 返回 key 对应的人类可读目标位置，用于日志。
func (s *s3Store) describe(key string) string {
	return fmt.Sprintf("s3://%s/%s", s.bucket, s.objectName(key))
}
