package main

import (
	"context"
	"fmt"
	"gRPC-GO/blog/blogpb"
	"log"

	"google.golang.org/grpc"
)

func main() {
	fmt.Println("Blog client")

	opts := grpc.WithInsecure()

	cc, err := grpc.Dial("localhost:50051", opts)
	if err != nil {
		log.Fatalf("couldn't connect: %v", err)
	}

	defer cc.Close()

	c := blogpb.NewBlogServiceClient(cc)

	// create Blog
	fmt.Println("Creating blog")
	blog := &blogpb.Blog{
		AuthorId: "Adem",
		Title:    "First blog",
		Content:  "gRPC + golang API",
	}

	createdBlogRes, err := c.CreateBlog(context.Background(), &blogpb.CreateBlogRequest{Blog: blog})
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	fmt.Printf("Blog Created: %v\n", createdBlogRes)

}
