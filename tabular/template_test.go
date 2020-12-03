package tabular

var template = `--- 
Memory: 
 Index:
  counter: rdb_index_and_filter_blocks_mem_usage
  unit: byte
 MemTable:
  counter: rdb_memtable_mem_usage
  unit: byte
Performance:
 Abnormal:
  counter: recent_abnormal_count
 Expire:
  counter: recent_expire_count
 Filter:
  counter: recent_filter_count
`

// func Test(t *testing.T) {

// 	tm := NewTemplate(template)
// 	tm.SetColumnValueFunc(func(col *ColumnAttributes, rowData interface{}) interface{} {
// 		return 1
// 	})
// 	for _, sec := range tm.sections {
// 		for _, col := range sec.columns {
// 			assert.Equal(t, col.formatter, defaultFormatter)
// 		}
// 	}
// 	fmt.Printf("%v\n", tm.sections)

// 	t.Fail()
// }
