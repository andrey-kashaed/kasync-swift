//
// DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
//
// Copyright (c) 2023 Andrey Kashaed. All rights reserved.
//
// The contents of this file are subject to the terms of the
// Common Development and Distribution License 1.0 (the "License").
// You may not use this file except in compliance with the License.
//
// You can obtain a copy of the License at
// https://opensource.org/licenses/CDDL-1.0 or LICENSE.txt.
//

@propertyWrapper
public struct Atomic<Operation: Sendable, BlockingOperation: Sendable>: Sendable {
    
    private var atomicOperation: Operation
    private var atomicBlockingOperation: BlockingOperation
    
    public var wrappedValue: Operation {
        atomicOperation
    }
    
    public var projectedValue: BlockingOperation {
        atomicBlockingOperation
    }
    
    public init<R: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable () async throws -> R, BlockingOperation == @Sendable () throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable in
            try await mutex.atomic {
                try await operation()
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = {
            try runBlocking {
                try await atomicOperation()
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1) async throws -> R, BlockingOperation == @Sendable (P1) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1 in
            try await mutex.atomic {
                try await operation(p1)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1 in
            try runBlocking {
                try await atomicOperation(p1)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2) async throws -> R, BlockingOperation == @Sendable (P1, P2) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2 in
            try await mutex.atomic {
                try await operation(p1, p2)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2 in
            try runBlocking {
                try await atomicOperation(p1, p2)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable, P3: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2, P3) async throws -> R, BlockingOperation == @Sendable (P1, P2, P3) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2, p3 in
            try await mutex.atomic {
                try await operation(p1, p2, p3)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2, p3 in
            try runBlocking {
                try await atomicOperation(p1, p2, p3)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable, P3: Sendable, P4: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2, P3, P4) async throws -> R, BlockingOperation == @Sendable (P1, P2, P3, P4) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2, p3, p4 in
            try await mutex.atomic {
                try await operation(p1, p2, p3, p4)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2, p3, p4 in
            try runBlocking {
                try await atomicOperation(p1, p2, p3, p4)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable, P3: Sendable, P4: Sendable, P5: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2, P3, P4, P5) async throws -> R, BlockingOperation == @Sendable (P1, P2, P3, P4, P5) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2, p3, p4, p5 in
            try await mutex.atomic {
                try await operation(p1, p2, p3, p4, p5)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2, p3, p4, p5 in
            try runBlocking {
                try await atomicOperation(p1, p2, p3, p4, p5)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable, P3: Sendable, P4: Sendable, P5: Sendable, P6: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2, P3, P4, P5, P6) async throws -> R, BlockingOperation == @Sendable (P1, P2, P3, P4, P5, P6) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2, p3, p4, p5, p6 in
            try await mutex.atomic {
                try await operation(p1, p2, p3, p4, p5, p6)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2, p3, p4, p5, p6 in
            try runBlocking {
                try await atomicOperation(p1, p2, p3, p4, p5, p6)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable, P3: Sendable, P4: Sendable, P5: Sendable, P6: Sendable, P7: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2, P3, P4, P5, P6, P7) async throws -> R, BlockingOperation == @Sendable (P1, P2, P3, P4, P5, P6, P7) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2, p3, p4, p5, p6, p7 in
            try await mutex.atomic {
                try await operation(p1, p2, p3, p4, p5, p6, p7)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2, p3, p4, p5, p6, p7 in
            try runBlocking {
                try await atomicOperation(p1, p2, p3, p4, p5, p6, p7)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable, P3: Sendable, P4: Sendable, P5: Sendable, P6: Sendable, P7: Sendable, P8: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2, P3, P4, P5, P6, P7, P8) async throws -> R, BlockingOperation == @Sendable (P1, P2, P3, P4, P5, P6, P7, P8) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2, p3, p4, p5, p6, p7, p8 in
            try await mutex.atomic {
                try await operation(p1, p2, p3, p4, p5, p6, p7, p8)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2, p3, p4, p5, p6, p7, p8 in
            try runBlocking {
                try await atomicOperation(p1, p2, p3, p4, p5, p6, p7, p8)
            }
        }
    }
    
    public init<R: Sendable, P1: Sendable, P2: Sendable, P3: Sendable, P4: Sendable, P5: Sendable, P6: Sendable, P7: Sendable, P8: Sendable, P9: Sendable>(wrappedValue operation: Operation) where Operation == @Sendable (P1, P2, P3, P4, P5, P6, P7, P8, P9) async throws -> R, BlockingOperation == @Sendable (P1, P2, P3, P4, P5, P6, P7, P8, P9) throws -> R {
        let mutex = Mutex()
        let atomicOperation = { @Sendable p1, p2, p3, p4, p5, p6, p7, p8, p9 in
            try await mutex.atomic {
                try await operation(p1, p2, p3, p4, p5, p6, p7, p8, p9)
            }
        }
        self.atomicOperation = atomicOperation
        self.atomicBlockingOperation = { p1, p2, p3, p4, p5, p6, p7, p8, p9 in
            try runBlocking {
                try await atomicOperation(p1, p2, p3, p4, p5, p6, p7, p8, p9)
            }
        }
    }
    
}
