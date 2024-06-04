// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestChangePumpAndDrainer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// change pump or drainer's state need connect to etcd
	// so will meet error "URL scheme must be http, https, unix, or unixs: /tmp/tidb"
	tk.MustMatchErrMsg("change pump to node_state ='paused' for node_id 'pump1'", "URL scheme must be http, https, unix, or unixs.*")
	tk.MustMatchErrMsg("change drainer to node_state ='paused' for node_id 'drainer1'", "URL scheme must be http, https, unix, or unixs.*")
}

func TestHashJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, key(a));")
	tk.MustExec("create table t2 (a int, b int, key(a));")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3);")

	tk.MustQuery("select /*+ HASH_JOIN(t1, t2) */ * from t1 join t2 on t1.a = t2.a;").Check(testkit.Rows("1 1 1 1", "2 2 2 2", "3 3 3 3"))
}
