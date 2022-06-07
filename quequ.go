package quequ

// minRoundNumBy2 round 到 >=N的 最近的2的倍数，
// example f(3) = 4
func minRoundNumBy2(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}
