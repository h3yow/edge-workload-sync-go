package edgesync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// FieldDiff 描述云端权威内容与边缘实际运行内容在一个字段上的差异。
type FieldDiff struct {
	Field string `json:"field"` // 字段名（version/image/replicas/protected）
	Cloud string `json:"cloud"` // 云端权威值
	Edge  string `json:"edge"`  // 边缘实际运行值
}

// ConsistencyReport 是某工作负载云端权威内容与边缘实际运行内容的一致性结论。
type ConsistencyReport struct {
	Workload    string      `json:"workload"`
	Consistent  bool        `json:"consistent"`      // 两端内容是否一致
	CloudFound  bool        `json:"cloud_found"`     // 云端是否存在该工作负载
	EdgeFound   bool        `json:"edge_found"`      // 边缘是否已应用该工作负载
	CloudDigest string      `json:"cloud_digest"`    // 云端权威内容摘要
	EdgeDigest  string      `json:"edge_digest"`     // 边缘运行内容摘要
	Diffs       []FieldDiff `json:"diffs,omitempty"` // 不一致时的逐字段差异
}

// digestOf 计算期望状态内容（版本+规格）的稳定摘要，
// 用于快速识别"版本号没变但内容对不上"的跑偏。
func digestOf(d DesiredState) string {
	raw, _ := json.Marshal(struct {
		Version Version      `json:"version"`
		Spec    WorkloadSpec `json:"spec"`
	}{d.Version, d.Spec})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

// VerifyConsistency 比较云端权威期望状态与边缘实际应用状态，
// 返回可区分的一致性结论：两端都存在且内容一致时 Consistent 为 true；
// 不一致时 Diffs 给出逐字段差异（含版本、镜像、副本数、保护标记），
// 任一端缺失也会使结论为不一致并在 CloudFound/EdgeFound 中体现。
func VerifyConsistency(cloud *Cloud, edge *Edge, workload string) ConsistencyReport {
	cd, cok := cloud.Desired(workload)
	ed, eok := edge.Current(workload)
	rep := ConsistencyReport{
		Workload:   workload,
		CloudFound: cok,
		EdgeFound:  eok,
	}
	if cok {
		rep.CloudDigest = digestOf(cd)
	}
	if eok {
		rep.EdgeDigest = digestOf(ed)
	}
	if !cok || !eok {
		return rep
	}
	if cd.Version != ed.Version {
		rep.Diffs = append(rep.Diffs, FieldDiff{
			Field: "version",
			Cloud: fmt.Sprintf("%d", cd.Version),
			Edge:  fmt.Sprintf("%d", ed.Version),
		})
	}
	if cd.Spec.Image != ed.Spec.Image {
		rep.Diffs = append(rep.Diffs, FieldDiff{Field: "image", Cloud: cd.Spec.Image, Edge: ed.Spec.Image})
	}
	if cd.Spec.Replicas != ed.Spec.Replicas {
		rep.Diffs = append(rep.Diffs, FieldDiff{
			Field: "replicas",
			Cloud: fmt.Sprintf("%d", cd.Spec.Replicas),
			Edge:  fmt.Sprintf("%d", ed.Spec.Replicas),
		})
	}
	if cd.Spec.Protected != ed.Spec.Protected {
		rep.Diffs = append(rep.Diffs, FieldDiff{
			Field: "protected",
			Cloud: fmt.Sprintf("%t", cd.Spec.Protected),
			Edge:  fmt.Sprintf("%t", ed.Spec.Protected),
		})
	}
	rep.Consistent = len(rep.Diffs) == 0
	return rep
}
