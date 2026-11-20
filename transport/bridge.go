package transport

// Bridge forwards every frame received on one transport to all the others
// until a transport fails, and returns that error. It is what `goros
// bridge` runs: the goros analogue of the micro-ROS agent, joining a
// serial-attached microcontroller to the UDP domain.
func Bridge(links ...Transport) error {
	errc := make(chan error, len(links))
	for i := range links {
		src := links[i]
		go func() {
			for {
				buf, err := src.Recv()
				if err != nil {
					errc <- err
					return
				}
				if len(buf) < 4 || buf[0] != Magic0 || buf[1] != Magic1 {
					continue
				}
				for _, dst := range links {
					if dst != src {
						_ = dst.Send(buf)
					}
				}
			}
		}()
	}
	return <-errc
}
