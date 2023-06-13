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
public final class UncheckedReference<Value>: @unchecked Sendable {
    
    fileprivate var value: Value

    public init(wrappedValue value: Value) {
        self.value = value
    }

    public var wrappedValue: Value {
        value
    }
    
    public var projectedValue: UncheckedReference<Value> {
        self
    }
    
}

infix operator =^

public func =^<Value>(left: UncheckedReference<Value>, right: Value) {
    left.value = right
}
