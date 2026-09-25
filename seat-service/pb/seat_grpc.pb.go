package pb

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

const _ = grpc.SupportPackageIsVersion9

const (
	SeatService_GetSeats_FullMethodName    = "/pb.SeatService/GetSeats"
	SeatService_ReserveSeat_FullMethodName = "/pb.SeatService/ReserveSeat"
	SeatService_ReleaseSeat_FullMethodName = "/pb.SeatService/ReleaseSeat"
)

type SeatServiceClient interface {
	GetSeats(ctx context.Context, in *GetSeatsRequest, opts ...grpc.CallOption) (*GetSeatsResponse, error)
	ReserveSeat(ctx context.Context, in *ReserveSeatRequest, opts ...grpc.CallOption) (*ReserveSeatResponse, error)
	ReleaseSeat(ctx context.Context, in *ReleaseSeatRequest, opts ...grpc.CallOption) (*ReleaseSeatResponse, error)
}

type seatServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSeatServiceClient(cc grpc.ClientConnInterface) SeatServiceClient {
	return &seatServiceClient{cc}
}

func (c *seatServiceClient) GetSeats(ctx context.Context, in *GetSeatsRequest, opts ...grpc.CallOption) (*GetSeatsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetSeatsResponse)
	err := c.cc.Invoke(ctx, SeatService_GetSeats_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seatServiceClient) ReserveSeat(ctx context.Context, in *ReserveSeatRequest, opts ...grpc.CallOption) (*ReserveSeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReserveSeatResponse)
	err := c.cc.Invoke(ctx, SeatService_ReserveSeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *seatServiceClient) ReleaseSeat(ctx context.Context, in *ReleaseSeatRequest, opts ...grpc.CallOption) (*ReleaseSeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReleaseSeatResponse)
	err := c.cc.Invoke(ctx, SeatService_ReleaseSeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type SeatServiceServer interface {
	GetSeats(context.Context, *GetSeatsRequest) (*GetSeatsResponse, error)
	ReserveSeat(context.Context, *ReserveSeatRequest) (*ReserveSeatResponse, error)
	ReleaseSeat(context.Context, *ReleaseSeatRequest) (*ReleaseSeatResponse, error)
	mustEmbedUnimplementedSeatServiceServer()
}

type UnimplementedSeatServiceServer struct{}

func (UnimplementedSeatServiceServer) GetSeats(context.Context, *GetSeatsRequest) (*GetSeatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetSeats not implemented")
}
func (UnimplementedSeatServiceServer) ReserveSeat(context.Context, *ReserveSeatRequest) (*ReserveSeatResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ReserveSeat not implemented")
}
func (UnimplementedSeatServiceServer) ReleaseSeat(context.Context, *ReleaseSeatRequest) (*ReleaseSeatResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ReleaseSeat not implemented")
}
func (UnimplementedSeatServiceServer) mustEmbedUnimplementedSeatServiceServer() {}
func (UnimplementedSeatServiceServer) testEmbeddedByValue()                     {}

type UnsafeSeatServiceServer interface {
	mustEmbedUnimplementedSeatServiceServer()
}

func RegisterSeatServiceServer(s grpc.ServiceRegistrar, srv SeatServiceServer) {
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&SeatService_ServiceDesc, srv)
}

func _SeatService_GetSeats_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetSeatsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeatServiceServer).GetSeats(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SeatService_GetSeats_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeatServiceServer).GetSeats(ctx, req.(*GetSeatsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeatService_ReserveSeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReserveSeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeatServiceServer).ReserveSeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SeatService_ReserveSeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeatServiceServer).ReserveSeat(ctx, req.(*ReserveSeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SeatService_ReleaseSeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReleaseSeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SeatServiceServer).ReleaseSeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SeatService_ReleaseSeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SeatServiceServer).ReleaseSeat(ctx, req.(*ReleaseSeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var SeatService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pb.SeatService",
	HandlerType: (*SeatServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetSeats",
			Handler:    _SeatService_GetSeats_Handler,
		},
		{
			MethodName: "ReserveSeat",
			Handler:    _SeatService_ReserveSeat_Handler,
		},
		{
			MethodName: "ReleaseSeat",
			Handler:    _SeatService_ReleaseSeat_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/seat.proto",
}
