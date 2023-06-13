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
public struct AtomicReference<Value: Sendable>: Sendable {
    
    public actor Safeguard {
        
        private var value: Value
        
        fileprivate init(value: Value) {
            self.value = value
        }
        
        fileprivate func getValue() -> Value {
            value
        }
        
        fileprivate func setValue(value: Value) {
            self.value = value
        }
        
        @discardableResult
        public func atomic<R>(_ operation: @Sendable (inout Value) -> R) -> R {
            return operation(&value)
        }
        
    }
    
    public struct BlockingSafeguard: Sendable {
        
        private var safeguard: Safeguard
        
        fileprivate init(safeguard: Safeguard) {
            self.safeguard = safeguard
        }
        
        fileprivate func getValue() -> Value {
            runBlocking { await safeguard.getValue() }
        }
        
        fileprivate func setValue(value: Value) {
            runBlocking { await safeguard.setValue(value: value) }
        }
        
        @discardableResult
        public func atomic<R: Sendable>(_ operation: @Sendable @escaping (inout Value) -> R) -> R {
            return runBlocking { await safeguard.atomic(operation) }
        }
        
    }
    
    private var safeguard: Safeguard
    private var blockingSafeguard: BlockingSafeguard

    public init(_ value: Value) {
        safeguard = Safeguard(value: value)
        blockingSafeguard = BlockingSafeguard(safeguard: safeguard)
    }

    public var wrappedValue: Safeguard {
        safeguard
    }
    
    public var projectedValue: BlockingSafeguard {
        blockingSafeguard
    }
    
}

postfix operator ^

public postfix func ^<Value>(left: AtomicReference<Value>.Safeguard) async -> Value {
    await left.getValue()
}

public postfix func ^<Value>(left: AtomicReference<Value>.BlockingSafeguard) -> Value {
    left.getValue()
}

infix operator =^

public func =^<Value>(left: AtomicReference<Value>.Safeguard, right: Value) async {
    await left.setValue(value: right)
}

public func =^<Value>(left: AtomicReference<Value>.BlockingSafeguard, right: Value) {
    left.setValue(value: right)
}
