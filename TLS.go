package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"os"
	"time"
)

func main() {
	// 生成RSA私钥
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("无法生成私钥: %v", err)
	}

	// 序列号必须是唯一的
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		log.Fatalf("无法生成序列号: %v", err)
	}

	// 设置证书有效期
	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour) // 一年有效期

	// 创建证书模板
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Quic HTTP/3 File Upload Example"},
			CommonName:   "localhost",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	// 创建自签名证书
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		log.Fatalf("无法创建证书: %v", err)
	}

	// 将证书写入文件
	certOut, err := os.Create("cert.pem")
	if err != nil {
		log.Fatalf("无法创建cert.pem: %v", err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		log.Fatalf("无法将证书写入cert.pem: %v", err)
	}
	if err := certOut.Close(); err != nil {
		log.Fatalf("无法关闭cert.pem: %v", err)
	}
	log.Print("证书已写入cert.pem")

	// 将私钥写入文件
	keyOut, err := os.OpenFile("key.pem", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("无法创建key.pem: %v", err)
	}
	privBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		log.Fatalf("无法序列化私钥: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		log.Fatalf("无法将私钥写入key.pem: %v", err)
	}
	if err := keyOut.Close(); err != nil {
		log.Fatalf("无法关闭key.pem: %v", err)
	}
	log.Print("私钥已写入key.pem")
	log.Print("证书和私钥已成功生成！")
}
