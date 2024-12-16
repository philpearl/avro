package avro

func skip(r *ReadBuf, l int64) error {
	_, err := r.Next(int(l))
	return err
}
